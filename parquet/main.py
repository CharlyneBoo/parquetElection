from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(title="API Municipales 2026", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "municipales.duckdb"
TABLE_NAME = "municipales_2026"
MAX_LISTES = 13


def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


def dept_code_expr() -> str:
    return """
    CASE
        WHEN code_departement IS NULL THEN NULL
        WHEN TRY_CAST(code_departement AS VARCHAR) IS NULL THEN NULL
        ELSE LPAD(CAST(code_departement AS VARCHAR), 2, '0')
    END
    """


def commune_code_expr() -> str:
    return """
    CASE
        WHEN code_commune IS NULL THEN NULL
        WHEN TRY_CAST(code_commune AS VARCHAR) IS NULL THEN NULL
        ELSE LPAD(CAST(code_commune AS VARCHAR), 5, '0')
    END
    """


def region_code_expr() -> str:
    dept = dept_code_expr()
    return f"""
    CASE
        WHEN {dept} IN ('01','03','07','15','26','38','42','43','63','69','73','74') THEN '84'
        WHEN {dept} IN ('21','25','39','58','70','71','89','90') THEN '27'
        WHEN {dept} IN ('22','29','35','56') THEN '53'
        WHEN {dept} IN ('18','28','36','37','41','45') THEN '24'
        WHEN {dept} IN ('2A','2B') THEN '94'
        WHEN {dept} IN ('08','10','51','52','54','55','57','67','68','88') THEN '44'
        WHEN {dept} IN ('02','59','60','62','80') THEN '32'
        WHEN {dept} IN ('14','27','50','61','76') THEN '28'
        WHEN {dept} IN ('16','17','19','23','24','33','40','47','64','79','86','87') THEN '75'
        WHEN {dept} IN ('09','11','12','30','31','32','34','46','48','65','66','81','82') THEN '76'
        WHEN {dept} IN ('44','49','53','72','85') THEN '52'
        WHEN {dept} IN ('04','05','06','13','83','84') THEN '93'
        WHEN {dept} IN ('75','77','78','91','92','93','94','95') THEN '11'
        WHEN {dept} = '971' THEN '01'
        WHEN {dept} = '972' THEN '02'
        WHEN {dept} = '973' THEN '03'
        WHEN {dept} = '974' THEN '04'
        WHEN {dept} = '976' THEN '06'
        ELSE NULL
    END
    """


def region_label_expr() -> str:
    dept = dept_code_expr()
    return f"""
    CASE
        WHEN {dept} IN ('01','03','07','15','26','38','42','43','63','69','73','74') THEN 'Auvergne-Rhône-Alpes'
        WHEN {dept} IN ('21','25','39','58','70','71','89','90') THEN 'Bourgogne-Franche-Comté'
        WHEN {dept} IN ('22','29','35','56') THEN 'Bretagne'
        WHEN {dept} IN ('18','28','36','37','41','45') THEN 'Centre-Val de Loire'
        WHEN {dept} IN ('2A','2B') THEN 'Corse'
        WHEN {dept} IN ('08','10','51','52','54','55','57','67','68','88') THEN 'Grand Est'
        WHEN {dept} IN ('02','59','60','62','80') THEN 'Hauts-de-France'
        WHEN {dept} IN ('14','27','50','61','76') THEN 'Normandie'
        WHEN {dept} IN ('16','17','19','23','24','33','40','47','64','79','86','87') THEN 'Nouvelle-Aquitaine'
        WHEN {dept} IN ('09','11','12','30','31','32','34','46','48','65','66','81','82') THEN 'Occitanie'
        WHEN {dept} IN ('44','49','53','72','85') THEN 'Pays de la Loire'
        WHEN {dept} IN ('04','05','06','13','83','84') THEN 'Provence-Alpes-Côte d''Azur'
        WHEN {dept} IN ('75','77','78','91','92','93','94','95') THEN 'Île-de-France'
        WHEN {dept} = '971' THEN 'Guadeloupe'
        WHEN {dept} = '972' THEN 'Martinique'
        WHEN {dept} = '973' THEN 'Guyane'
        WHEN {dept} = '974' THEN 'La Réunion'
        WHEN {dept} = '976' THEN 'Mayotte'
        ELSE 'Inconnue'
    END
    """


LEVELS = {
    "commune": {
        "code_expr": commune_code_expr(),
        "label_expr": "libelle_commune",
    },
    "departement": {
        "code_expr": dept_code_expr(),
        "label_expr": "libelle_departement",
    },
    "region": {
        "code_expr": region_code_expr(),
        "label_expr": region_label_expr(),
    },
    "france": {
        "code_expr": "'FR'",
        "label_expr": "'France'",
    },
}


def build_where(level: str, code: Optional[str], tour: Optional[int]):
    if level not in LEVELS:
        raise HTTPException(status_code=400, detail="level invalide")

    clauses = []
    params = []

    if tour is not None:
        clauses.append("CAST(tour AS VARCHAR) = ?")
        params.append(str(tour))

    if level != "france":
        if not code:
            raise HTTPException(status_code=400, detail="code requis pour ce niveau")
        clauses.append(f"{LEVELS[level]['code_expr']} = ?")
        params.append(code)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    return where_sql, params


def candidate_union_sql(level: str, where_sql: str) -> str:
    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]

    parts = []
    for i in range(1, MAX_LISTES + 1):
        parts.append(f"""
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            COALESCE(
                NULLIF(TRIM(CAST(nuance_liste_{i} AS VARCHAR)), ''),
                'NON RENSEIGNEE'
            ) AS etiquette,
            TRY_CAST(voix_{i} AS BIGINT) AS voix
        FROM {TABLE_NAME}
        {where_sql}
        """)
    return "\nUNION ALL\n".join(parts)


def sieges_union_sql(level: str, where_sql: str) -> str:
    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]

    parts = []
    for i in range(1, MAX_LISTES + 1):
        parts.append(f"""
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            COALESCE(NULLIF(TRIM(CAST(nuance_liste_{i} AS VARCHAR)), ''), 'NON RENSEIGNEE') AS nuance,
            TRY_CAST(sieges_au_cm_{i} AS BIGINT) AS sieges_cm,
            TRY_CAST(sieges_au_cc_{i} AS BIGINT) AS sieges_cc
        FROM {TABLE_NAME}
        {where_sql}
        """)
    return "\nUNION ALL\n".join(parts)


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <title>Résultats Municipales 2026</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100 p-4 md:p-8 font-sans">
        <div class="max-w-6xl mx-auto space-y-6">

            <div class="flex gap-4 items-end">
                <div>
                    <label class="block text-sm font-bold text-gray-600 mb-1 uppercase tracking-wider">
                        Commune
                    </label>
                    <input type="text" id="inseeInput" value="75056"
                        class="border-2 border-gray-100 bg-gray-50 p-3 rounded-lg w-full focus:bg-white focus:border-blue-500 outline-none transition-all text-lg font-mono">
                </div>

                <div>
                    <label class="block text-sm font-bold text-gray-600 mb-1 uppercase tracking-wider">
                        Tour
                    </label>
                    <select id="tourSelect"
                            class="border-2 border-gray-100 bg-gray-50 p-3 rounded-lg focus:bg-white focus:border-blue-500 outline-none transition-all">
                        <option value="1">1er tour</option>
                        <option value="2" selected>2e tour</option>
                    </select>
                </div>

                <button onclick="fetchData()"
                        class="bg-blue-600 text-white px-10 py-3.5 rounded-lg font-bold hover:bg-blue-700 shadow-lg shadow-blue-200 transition-all transform active:scale-95">
                    CHARGER LES RÉSULTATS
                </button>
            </div>

            <div id="resultsContent" class="hidden space-y-8">
                <div class="bg-white p-8 rounded-xl shadow-xl border border-gray-100">
                    <h1 class="text-4xl font-black text-gray-900 mb-8 border-b-8 border-blue-600 pb-4 inline-block uppercase" id="cityName">Commune</h1>

                    <h2 class="text-2xl font-bold mb-5 text-gray-800 flex items-center">
                        <span class="w-2 h-8 bg-blue-600 mr-3 rounded-full"></span>
                        Liste des candidatures
                    </h2>
                    <div class="overflow-x-auto mb-12">
                        <table class="w-full border-collapse border border-gray-200 shadow-sm">
                            <thead>
                                <tr class="bg-gray-800 text-white">
                                    <th class="p-3 text-left border border-gray-700">Liste</th>
                                    <th class="p-3 text-left border border-gray-700">Conduite par</th>
                                    <th class="p-3 text-center border border-gray-700">Nuance</th>
                                    <th class="p-3 text-right border border-gray-700">Voix</th>
                                    <th class="p-3 text-center border border-gray-700">% Ins.</th>
                                    <th class="p-3 text-center border border-gray-700">% Exp.</th>
                                    <th class="p-3 text-center border border-gray-700 bg-blue-900">Sièges CM</th>
                                    <th class="p-3 text-center border border-gray-700 bg-blue-900">Sièges CC</th>
                                </tr>
                            </thead>
                            <tbody id="candidatsTable" class="text-gray-700"></tbody>
                        </table>
                    </div>

                    <h2 class="text-2xl font-bold mb-5 text-gray-800 flex items-center">
                        <span class="w-2 h-8 bg-gray-400 mr-3 rounded-full"></span>
                        Mentions globales
                    </h2>
                    <div class="max-w-3xl overflow-x-auto">
                        <table class="w-full border-collapse border border-gray-200 shadow-sm">
                            <thead>
                                <tr class="bg-gray-100 text-gray-700">
                                    <th class="p-3 text-left border border-gray-300">Catégorie</th>
                                    <th class="p-3 text-right border border-gray-300">Nombre</th>
                                    <th class="p-3 text-center border border-gray-300">% Inscrits</th>
                                    <th class="p-3 text-center border border-gray-300">% Votants</th>
                                </tr>
                            </thead>
                            <tbody id="mentionsTable"></tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <script>
        async function fetchData() {
            const insee = document.getElementById('inseeInput').value.trim();
            const tour = document.getElementById('tourSelect').value;

            try {
                const res = await fetch(`/commune_resume?code=${insee}&tour=${tour}`);
                const data = await res.json();

                if (!res.ok) {
                    alert("Erreur : " + (data.detail || "Données introuvables"));
                    return;
                }

                document.getElementById('resultsContent').classList.remove('hidden');
                document.getElementById('cityName').innerText = `${data.commune} — ${tour}e tour`;

                const cBody = document.getElementById('candidatsTable');
                cBody.innerHTML = '';
                data.candidatures.forEach(c => {
                    const tr = document.createElement('tr');
                    tr.className = "hover:bg-blue-50 border-b border-gray-100 transition-colors";
                    tr.innerHTML = `
                        <td class="p-3 border border-gray-200 font-bold text-blue-900 uppercase text-xs">${c.liste ?? ''}</td>
                        <td class="p-3 border border-gray-200 font-medium">${c.conduite_par ?? ''}</td>
                        <td class="p-3 border border-gray-200 text-center font-mono text-xs">${c.nuance ?? ''}</td>
                        <td class="p-3 border border-gray-200 text-right font-bold text-gray-800">${Number(c.voix ?? 0).toLocaleString('fr-FR')}</td>
                        <td class="p-3 border border-gray-200 text-center text-gray-500">${Number(c.pct_inscrits ?? 0).toFixed(2)} %</td>
                        <td class="p-3 border border-gray-200 text-center text-gray-800 font-semibold">${Number(c.pct_exprimes ?? 0).toFixed(2)} %</td>
                        <td class="p-3 border border-gray-200 text-center font-black text-blue-700 bg-blue-50 text-lg">${c.cm ?? 0}</td>
                        <td class="p-3 border border-gray-200 text-center font-bold text-blue-600 bg-blue-50">${c.cc ?? 0}</td>
                    `;
                    cBody.appendChild(tr);
                });

                const m = data.mentions;
                const rows = [
                    { label: "Inscrits", nombre: m.inscrits, pct_inscrits: 100.0, pct_votants: null },
                    { label: "Abstentions", nombre: m.abstentions, pct_inscrits: m.inscrits ? (100 * m.abstentions / m.inscrits) : 0, pct_votants: null },
                    { label: "Votants", nombre: m.votants, pct_inscrits: m.inscrits ? (100 * m.votants / m.inscrits) : 0, pct_votants: 100.0 },
                    { label: "Blancs", nombre: m.blancs, pct_inscrits: m.inscrits ? (100 * m.blancs / m.inscrits) : 0, pct_votants: m.votants ? (100 * m.blancs / m.votants) : 0 },
                    { label: "Nuls", nombre: m.nuls, pct_inscrits: m.inscrits ? (100 * m.nuls / m.inscrits) : 0, pct_votants: m.votants ? (100 * m.nuls / m.votants) : 0 },
                    { label: "Exprimés", nombre: m.exprimes, pct_inscrits: m.inscrits ? (100 * m.exprimes / m.inscrits) : 0, pct_votants: m.votants ? (100 * m.exprimes / m.votants) : 0 }
                ];

                const mBody = document.getElementById('mentionsTable');
                mBody.innerHTML = '';
                rows.forEach(row => {
                    const tr = document.createElement('tr');
                    tr.className = "hover:bg-gray-50 border-b border-gray-200";
                    tr.innerHTML = `
                        <td class="p-3 border border-gray-200 font-bold text-gray-700">${row.label}</td>
                        <td class="p-3 border border-gray-200 text-right font-mono">${Number(row.nombre ?? 0).toLocaleString('fr-FR')}</td>
                        <td class="p-3 border border-gray-200 text-center text-gray-500">${row.pct_inscrits !== null ? Number(row.pct_inscrits).toFixed(2) + ' %' : '-'}</td>
                        <td class="p-3 border border-gray-200 text-center text-gray-500">${row.pct_votants !== null ? Number(row.pct_votants).toFixed(2) + ' %' : '-'}</td>
                    `;
                    mBody.appendChild(tr);
                });

            } catch (err) {
                console.error(err);
                alert("Erreur de communication avec l'API");
            }
        }

        window.onload = fetchData;
    </script>
    </body>
    </html>
    """


@app.get("/health")
def health():
    con = get_conn()
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        return {"status": "ok", "rows": count}
    finally:
        con.close()


@app.get("/schema")
def schema():
    con = get_conn()
    try:
        rows = con.execute(f"DESCRIBE {TABLE_NAME}").fetchdf().to_dict(orient="records")
        return {"table": TABLE_NAME, "columns": rows}
    finally:
        con.close()


@app.get("/search_commune")
def search_commune(q: str = Query(..., description="nom ou partie du nom de commune")):
    sql = f"""
    SELECT DISTINCT
        {commune_code_expr()} AS code_commune,
        libelle_commune,
        {dept_code_expr()} AS code_departement,
        libelle_departement
    FROM {TABLE_NAME}
    WHERE libelle_commune ILIKE '%' || ? || '%'
    ORDER BY libelle_commune
    LIMIT 20
    """

    con = get_conn()
    try:
        df = con.execute(sql, [q]).fetchdf()
        return {"rows": df.to_dict(orient="records")}
    finally:
        con.close()


@app.get("/resultats")
def resultats(
    level: str = Query(..., description="commune|departement|region|france"),
    code: Optional[str] = Query(None, description="code territoire"),
    tour: Optional[int] = Query(None, description="1 ou 2"),
):
    if level not in LEVELS:
        raise HTTPException(status_code=400, detail="level invalide")

    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]
    where_sql, params = build_where(level, code, tour)
    union_sql = candidate_union_sql(level, where_sql)

    sql = f"""
    WITH base AS (
        SELECT *
        FROM {TABLE_NAME}
        {where_sql}
    ),
    bureaux AS (
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            CAST(code_bv AS VARCHAR) AS bureau,
            MAX(COALESCE(TRY_CAST(inscrits AS BIGINT), 0)) AS inscrits,
            MAX(COALESCE(TRY_CAST(votants AS BIGINT), 0)) AS votants,
            MAX(COALESCE(TRY_CAST(exprimes AS BIGINT), 0)) AS exprimes
        FROM base
        GROUP BY 1, 2, 3, 4
    ),
    totaux AS (
        SELECT
            tour,
            code,
            territoire,
            SUM(inscrits) AS inscrits,
            SUM(votants) AS votants,
            SUM(exprimes) AS exprimes
        FROM bureaux
        GROUP BY 1, 2, 3
    ),
    candidats AS (
        {union_sql}
    ),
    candidats_non_vides AS (
        SELECT *
        FROM candidats
        WHERE etiquette IS NOT NULL
        AND voix IS NOT NULL
        AND voix > 0
    )
    SELECT
        c.tour,
        c.code,
        c.territoire,
        c.etiquette,
        SUM(c.voix) AS voix,
        ROUND(100.0 * SUM(c.voix) / NULLIF(t.exprimes, 0), 2) AS pct_exprimes,
        ROUND(100.0 * SUM(c.voix) / NULLIF(t.inscrits, 0), 2) AS pct_inscrits
    FROM candidats_non_vides c
    JOIN totaux t
    ON c.tour = t.tour
    AND c.code = t.code
    AND c.territoire = t.territoire
    GROUP BY c.tour, c.code, c.territoire, c.etiquette, t.exprimes, t.inscrits
    ORDER BY c.tour, voix DESC, c.etiquette
    """

    con = get_conn()
    try:
        bind_params = params + (params * MAX_LISTES)
        df = con.execute(sql, bind_params).fetchdf()
        return {
            "level": level,
            "code": code,
            "tour": tour,
            "rows": df.to_dict(orient="records"),
        }
    finally:
        con.close()


@app.get("/participation")
def participation(
    level: str = Query(..., description="commune|departement|region|france"),
    code: Optional[str] = Query(None, description="code territoire"),
    tour: Optional[int] = Query(None, description="1 ou 2"),
):
    if level not in LEVELS:
        raise HTTPException(status_code=400, detail="level invalide")

    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]
    where_sql, params = build_where(level, code, tour)

    sql = f"""
    WITH base AS (
        SELECT *
        FROM {TABLE_NAME}
        {where_sql}
    ),
    bureaux AS (
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            CAST(code_bv AS VARCHAR) AS bureau,
            MAX(COALESCE(TRY_CAST(inscrits AS BIGINT), 0)) AS inscrits,
            MAX(COALESCE(TRY_CAST(votants AS BIGINT), 0)) AS votants,
            MAX(COALESCE(TRY_CAST(exprimes AS BIGINT), 0)) AS exprimes,
            MAX(COALESCE(TRY_CAST(abstentions AS BIGINT), 0)) AS abstentions
        FROM base
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        tour,
        code,
        territoire,
        SUM(inscrits) AS inscrits,
        SUM(votants) AS votants,
        SUM(exprimes) AS exprimes,
        SUM(abstentions) AS abstentions,
        ROUND(100.0 * SUM(votants) / NULLIF(SUM(inscrits), 0), 2) AS taux_participation,
        ROUND(100.0 * SUM(abstentions) / NULLIF(SUM(inscrits), 0), 2) AS taux_abstention
    FROM bureaux
    GROUP BY 1, 2, 3
    ORDER BY tour
    """

    con = get_conn()
    try:
        df = con.execute(sql, params * MAX_LISTES).fetchdf()
        return {
            "level": level,
            "code": code,
            "tour": tour,
            "rows": df.to_dict(orient="records"),
        }
    finally:
        con.close()


@app.get("/communes_gagnees_par_nuance")
def communes_gagnees_par_nuance(
    level: str = Query(..., description="departement|region|france"),
    code: Optional[str] = Query(None, description="code territoire"),
    tour: Optional[int] = Query(None, description="1 ou 2"),
):
    if level not in {"departement", "region", "france"}:
        raise HTTPException(status_code=400, detail="level doit être departement, region ou france")

    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]
    where_sql, params = build_where(level, code, tour)

    parts = []
    for i in range(1, MAX_LISTES + 1):
        parts.append(f"""
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            {commune_code_expr()} AS code_commune,
            libelle_commune,
            COALESCE(NULLIF(TRIM(CAST(nuance_liste_{i} AS VARCHAR)), ''), 'NON RENSEIGNEE') AS nuance,
            TRY_CAST(voix_{i} AS BIGINT) AS voix
        FROM {TABLE_NAME}
        {where_sql}
        """)
    union_sql = "\nUNION ALL\n".join(parts)

    sql = f"""
    WITH raw AS (
        {union_sql}
    ),
    agg AS (
        SELECT
            tour,
            code,
            territoire,
            code_commune,
            libelle_commune,
            nuance,
            SUM(COALESCE(voix, 0)) AS voix
        FROM raw
        WHERE code_commune IS NOT NULL
          AND nuance IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY tour, code, territoire, code_commune
                ORDER BY voix DESC, nuance
            ) AS rn
        FROM agg
    )
    SELECT
        tour,
        code,
        territoire,
        nuance,
        COUNT(*) AS nb_communes_gagnees
    FROM ranked
    WHERE rn = 1
    GROUP BY 1, 2, 3, 4
    ORDER BY nb_communes_gagnees DESC, nuance
    """

    con = get_conn()
    try:
        df = con.execute(sql, params * MAX_LISTES).fetchdf()
        return {
            "level": level,
            "code": code,
            "tour": tour,
            "rows": df.to_dict(orient="records"),
        }
    finally:
        con.close()


@app.get("/stats_sieges_par_commune_par_nuance")
def stats_sieges_par_commune_par_nuance(
    level: str = Query(..., description="departement|region|france"),
    code: Optional[str] = Query(None, description="code territoire"),
    tour: Optional[int] = Query(None, description="1 ou 2"),
):
    if level not in {"departement", "region", "france"}:
        raise HTTPException(status_code=400, detail="level doit être departement, region ou france")

    code_expr = LEVELS[level]["code_expr"]
    label_expr = LEVELS[level]["label_expr"]
    where_sql, params = build_where(level, code, tour)

    parts = []
    for i in range(1, MAX_LISTES + 1):
        parts.append(f"""
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {code_expr} AS code,
            {label_expr} AS territoire,
            {commune_code_expr()} AS code_commune,
            libelle_commune,
            COALESCE(NULLIF(TRIM(CAST(nuance_liste_{i} AS VARCHAR)), ''), 'NON RENSEIGNEE') AS nuance,
            TRY_CAST(sieges_au_cm_{i} AS BIGINT) AS sieges_cm
        FROM {TABLE_NAME}
        {where_sql}
        """)
    union_sql = "\nUNION ALL\n".join(parts)

    sql = f"""
    WITH raw AS (
        {union_sql}
    ),
    agg_commune AS (
        SELECT
            tour,
            code,
            territoire,
            code_commune,
            libelle_commune,
            nuance,
            SUM(COALESCE(sieges_cm, 0)) AS sieges_cm
        FROM raw
        WHERE code_commune IS NOT NULL
          AND nuance IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT
        tour,
        code,
        territoire,
        nuance,
        COUNT(*) AS nb_communes,
        ROUND(AVG(sieges_cm), 2) AS moyenne_sieges,
        CAST(MEDIAN(sieges_cm) AS DOUBLE) AS mediane_sieges,
        CAST(QUANTILE_CONT(sieges_cm, 0.25) AS DOUBLE) AS p25,
        CAST(QUANTILE_CONT(sieges_cm, 0.75) AS DOUBLE) AS p75,
        CAST(QUANTILE_CONT(sieges_cm, 0.90) AS DOUBLE) AS p90,
        CAST(QUANTILE_CONT(sieges_cm, 0.99) AS DOUBLE) AS p99
    FROM agg_commune
    GROUP BY 1, 2, 3, 4
    ORDER BY moyenne_sieges DESC, nuance
    """

    con = get_conn()
    try:
        df = con.execute(sql, params * MAX_LISTES).fetchdf()
        return {
            "level": level,
            "code": code,
            "tour": tour,
            "rows": df.to_dict(orient="records"),
        }
    finally:
        con.close()


@app.get("/commune_resume")
def commune_resume(
    code: str = Query(..., description="code INSEE commune, ex: 33422"),
    tour: int = Query(..., description="1 ou 2"),
):
    code = code.zfill(5)

    commune_expr = commune_code_expr()

    where_sql = f"""
    WHERE CAST(tour AS VARCHAR) = ?
      AND {commune_expr} = ?
    """

    # Identifiant de bureau plus robuste
    # On évite DISTINCT simple sur code_bv seul
    bureau_key_expr = """
    COALESCE(CAST(code_bv AS VARCHAR), '')
    """

    parts = []
    for i in range(1, MAX_LISTES + 1):
        parts.append(f"""
        SELECT
            CAST(tour AS VARCHAR) AS tour,
            {commune_expr} AS code_commune,
            libelle_commune,
            COALESCE(NULLIF(TRIM(CAST(libelle_de_liste_{i} AS VARCHAR)), ''), 'LISTE NON RENSEIGNEE') AS liste,
            NULLIF(
                TRIM(
                    COALESCE(CAST(prenom_candidat_{i} AS VARCHAR), '') || ' ' ||
                    COALESCE(CAST(nom_candidat_{i} AS VARCHAR), '')
                ),
                ''
            ) AS candidat,
            COALESCE(NULLIF(TRIM(CAST(nuance_liste_{i} AS VARCHAR)), ''), 'NON RENSEIGNEE') AS nuance,
            TRY_CAST(voix_{i} AS BIGINT) AS voix,
            TRY_CAST(sieges_au_cm_{i} AS BIGINT) AS cm,
            TRY_CAST(sieges_au_cc_{i} AS BIGINT) AS cc
        FROM {TABLE_NAME}
        {where_sql}
        """)
    union_sql = "\nUNION ALL\n".join(parts)

    sql = f"""
    WITH base AS (
        SELECT *
        FROM {TABLE_NAME}
        {where_sql}
    ),
    bureaux AS (
        SELECT
            {bureau_key_expr} AS bureau_key,
            MAX(libelle_commune) AS libelle_commune,
            MAX(COALESCE(TRY_CAST(inscrits AS BIGINT), 0)) AS inscrits,
            MAX(COALESCE(TRY_CAST(votants AS BIGINT), 0)) AS votants,
            MAX(COALESCE(TRY_CAST(abstentions AS BIGINT), 0)) AS abstentions,
            MAX(COALESCE(TRY_CAST(blancs AS BIGINT), 0)) AS blancs,
            MAX(COALESCE(TRY_CAST(nuls AS BIGINT), 0)) AS nuls,
            MAX(COALESCE(TRY_CAST(exprimes AS BIGINT), 0)) AS exprimes
        FROM base
        GROUP BY 1
    ),
    totaux AS (
        SELECT
            MAX(libelle_commune) AS commune,
            SUM(inscrits) AS inscrits,
            SUM(votants) AS votants,
            SUM(abstentions) AS abstentions,
            SUM(blancs) AS blancs,
            SUM(nuls) AS nuls,
            SUM(exprimes) AS exprimes
        FROM bureaux
    ),
    candidatures_raw AS (
        {union_sql}
    ),
    candidatures AS (
        SELECT
            code_commune,
            libelle_commune,
            liste,
            nuance,
            MAX(candidat) AS candidat,
            SUM(COALESCE(voix, 0)) AS voix,
            MAX(COALESCE(cm, 0)) AS cm,
            MAX(COALESCE(cc, 0)) AS cc
        FROM candidatures_raw
        WHERE voix IS NOT NULL
          AND voix > 0
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        c.code_commune,
        c.libelle_commune,
        c.liste,
        c.candidat,
        c.nuance,
        c.voix,
        c.cm,
        c.cc,
        t.commune,
        t.inscrits,
        t.votants,
        t.abstentions,
        t.blancs,
        t.nuls,
        t.exprimes
    FROM candidatures c
    CROSS JOIN totaux t
    ORDER BY c.voix DESC, c.liste
    """

    con = get_conn()
    try:
        params = [str(tour), code] + ([str(tour), code] * MAX_LISTES)
        df = con.execute(sql, params).fetchdf()

        if df.empty:
            raise HTTPException(status_code=404, detail="Aucun résultat")

        first = df.iloc[0]
        inscrits = int(first["inscrits"] or 0)
        votants = int(first["votants"] or 0)
        abstentions = int(first["abstentions"] or 0)
        blancs = int(first["blancs"] or 0)
        nuls = int(first["nuls"] or 0)
        exprimes = int(first["exprimes"] or 0)

        candidatures = []
        for _, row in df.iterrows():
            voix = int(row["voix"] or 0)
            candidatures.append({
                "liste": row["liste"],
                "conduite_par": row["candidat"],
                "nuance": row["nuance"],
                "voix": voix,
                "pct_inscrits": round(100 * voix / inscrits, 2) if inscrits else 0,
                "pct_exprimes": round(100 * voix / exprimes, 2) if exprimes else 0,
                "cm": int(row["cm"] or 0),
                "cc": int(row["cc"] or 0),
            })

        return {
            "commune": first["commune"],
            "code_commune": first["code_commune"],
            "tour": tour,
            "candidatures": candidatures,
            "mentions": {
                "inscrits": inscrits,
                "abstentions": abstentions,
                "votants": votants,
                "blancs": blancs,
                "nuls": nuls,
                "exprimes": exprimes,
            },
        }
    finally:
        con.close()