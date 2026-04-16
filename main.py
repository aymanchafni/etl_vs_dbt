from dbt_project import dbt
import scripts.dbgen as dbgen
import etl_project.etl as etl
import etl_project.etl_tests as etl_tests
import complexity
import lineage
import pandas as pd
import plotly.graph_objects as go

if __name__ == "__main__":
    print("================ PROJET ETL VS DBT ================")
    print("===================================================")
    print("================= MENU PRINCIPAL ==================\n")
    print("1. Générer les données TCPH-DBGEN et les sauvegarder dans des fichiers Parquet\n")
    print("2. Lancer le pipeline ETL (Python + DuckDB)\n")
    print("3. Lancer le pipeline ELT (dbt + DuckDB)\n")
    print("4. Comparer les performances des deux pipelines et afficher les résultats\n")
    print("5. Comparer la maintenabilité (tests de qualité ETL vs ELT)\n")
    print("6. Comparer la complexité de développement (LOC, CC, MI)\n")
    print("7. Comparer le data lineage et la documentation\n")
    print("8. Quitter\n")
    print("===================================================\n")

    choice = input("Veuillez entrer votre choix (1 à 8) : \n")

    while choice not in ["1", "2", "3", "4", "5", "6", "7", "8"]:
        choice = input("Choix invalide. Veuillez entrer un chiffre entre 1 et 8 : \n")

    match choice:
        case "1":
            scale_factor = input("Veuillez entrer l'échelle de génération des données (pour une echelle de 1, 1G de données sera générée) : ")
            
            while True:
                try:
                    scale_factor = int(scale_factor)
                    print("Génération des données TCPH-DBGEN et sauvegarde dans des fichiers Parquet...")
                    
                    if (dbgen.count_parquet_tables() > 0):
                        print("⚠️ Nous avons détecté que des fichiers Parquet existent déjà dans le dossier de données.")
                        overwrite_choice = input("Voulez-vous écraser les fichiers existants et générer les données à nouveau ? (y/n) : ")
                        
                        while overwrite_choice not in ["y", "n", "yes", "no"]:
                            overwrite_choice = input("Choix invalide. Veuillez entrer 'y/yes' pour oui ou 'n/no' pour non : ")
                        
                        if overwrite_choice == "yes" or overwrite_choice == "y":
                            dbgen.generate_data(scale_factor=scale_factor, overwrite=True)
                    else:
                        dbgen.generate_data(scale_factor=scale_factor, overwrite=False)

                    break
                except ValueError:
                    scale_factor = input("Échelle invalide. Veuillez entrer un nombre entier : ")
        case "2":
            etl.launch()
        case "3":
            dbt.launch()
        case "4":
            from plotly.subplots import make_subplots

            dbt_results = pd.read_json('dbt_benchmark_results.json')
            etl_results = pd.read_json('etl_benchmark_results.json')

            dbt_results['type'] = 'ELT (dbt)'
            etl_results['type'] = 'ETL (Pandas)'

            all_results = pd.concat([dbt_results, etl_results], ignore_index=True)

            transform_mask = all_results['analysis_name'].str.startswith('Transform')
            transform_df   = all_results[transform_mask].copy()
            analysis_df    = all_results[~transform_mask].copy()

            SCALE_FACTORS = sorted(all_results['scale_factor'].dropna().unique())
            COLORS        = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}
            PIPELINES     = ['ETL (Pandas)', 'ELT (dbt)']

            print("\n== Que voulez-vous comparer ? ==\n")
            print("1. Temps execution  — phase Transformation (par table, par scale)\n")
            print("2. Temps CPU        — phase Transformation (par table, par scale)\n")
            print("3. Debit            — phase Transformation (lignes/seconde, par scale)\n")
            print("4. Pic memoire      — phase Transformation (par table, par scale)\n")
            print("5. Temps execution  — phase Analyse (par requete, par scale)\n")
            print("6. Pic memoire      — phase Analyse (par requete, par scale)\n")
            print("7. Scalabilite      — evolution d'une metrique selon le scale factor\n")

            choice = input("Veuillez entrer votre choix (1 a 7) : \n")
            while choice not in ["1", "2", "3", "4", "5", "6", "7"]:
                choice = input("Choix invalide. Veuillez entrer un chiffre entre 1 et 7 : \n")

            def plot_grouped_by_scale(df, metric, ylabel, title):
                n_scales = len(SCALE_FACTORS)
                fig = make_subplots(
                    rows=1, cols=n_scales,
                    subplot_titles=[f"Scale {sf}" for sf in SCALE_FACTORS],
                    shared_yaxes=True,
                )
                for col, sf in enumerate(SCALE_FACTORS, start=1):
                    sf_df  = df[df['scale_factor'] == sf]
                    tables = list(sf_df['analysis_name'].unique())
                    for pipeline in PIPELINES:
                        subset = sf_df[sf_df['type'] == pipeline].set_index('analysis_name')
                        values = [float(subset.loc[t, metric]) if t in subset.index else 0 for t in tables]
                        fig.add_trace(go.Bar(
                            name         = pipeline,
                            x            = tables,
                            y            = values,
                            marker_color = COLORS[pipeline],
                            text         = [f"{v:,.3f}" for v in values],
                            textposition = 'outside',
                            showlegend   = (col == 1),
                            legendgroup  = pipeline,
                        ), row=1, col=col)
                    fig.update_xaxes(tickangle=-35, row=1, col=col)

                fig.update_layout(
                    title        = title,
                    yaxis_title  = ylabel,
                    barmode      = 'group',
                    legend       = dict(orientation="h", yanchor="bottom", y=1.05,
                                        xanchor="right", x=1),
                    hoverlabel   = dict(bgcolor="white", font_size=12),
                    template     = "plotly_white",
                    height       = 560,
                )
                fig.show()

            def plot_scalability(df, metric, ylabel, title):
                fig = go.Figure()
                for pipeline in PIPELINES:
                    sub = (df[df['type'] == pipeline]
                           .groupby('scale_factor')[metric]
                           .sum()
                           .reset_index()
                           .sort_values('scale_factor'))
                    fig.add_trace(go.Scatter(
                        name         = pipeline,
                        x            = sub['scale_factor'].tolist(),
                        y            = sub[metric].tolist(),
                        mode         = "lines+markers",
                        marker       = dict(size=10),
                        line         = dict(width=3),
                        marker_color = COLORS[pipeline],
                    ))
                fig.update_layout(
                    title        = title,
                    xaxis_title  = "Scale Factor",
                    yaxis_title  = ylabel,
                    xaxis        = dict(tickvals=SCALE_FACTORS),
                    legend       = dict(orientation="h", yanchor="bottom", y=1.02,
                                        xanchor="right", x=1),
                    template     = "plotly_white",
                    height       = 500,
                )
                fig.show()

            if choice == "1":
                plot_grouped_by_scale(transform_df, 'execution_time',
                    "Temps d'execution (s)",
                    "Phase Transform — Temps d'execution par table et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → ETL (Pandas) est plus lent sur fact_sales car les .merge() chargent tout en RAM.")
                print("  → ELT (dbt) délègue les jointures à DuckDB qui les exécute en C++ vectorisé.")
                print("  → L'écart s'accentue avec le scale factor : ETL ne passe pas à l'échelle aussi bien.")

            elif choice == "2":
                plot_grouped_by_scale(transform_df, 'cpu_time',
                    "Temps CPU (s)",
                    "Phase Transform — Temps CPU par table et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → ETL consomme plus de CPU car Python a un overhead par opération Pandas (GIL, copies).")
                print("  → ELT exécute le SQL dans DuckDB hors du thread Python, sans GIL.")
                print("  → Un CPU time ETL élevé = coût du traitement impératif vs déclaratif.")

            elif choice == "3":
                plot_grouped_by_scale(
                    transform_df[transform_df['throughput'].notna()], 'throughput',
                    "Lignes / seconde",
                    "Phase Transform — Debit par table et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → ELT affiche un débit plus élevé sur fact_sales : DuckDB traite en colonnes (OLAP).")
                print("  → ETL dégrade sur les grandes tables car .merge() copie les données en mémoire.")
                print("  → Pour les dim tables (petites), l'écart est négligeable.")

            elif choice == "4":
                plot_grouped_by_scale(transform_df, 'memory_peak',
                    "Pic memoire (MB)",
                    "Phase Transform — Pic memoire par table et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → ETL charge tout en RAM (Extract + Transform + Load simultanément).")
                print("  → ELT : les transformations SQL s'exécutent dans DuckDB sans passer par le heap Python.")
                print("  → La différence de mémoire est plus marquée à sf=1.0.")

            elif choice == "5":
                plot_grouped_by_scale(analysis_df, 'execution_time',
                    "Temps d'execution (s)",
                    "Phase Analyse — Temps d'execution par requete et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → Les deux pipelines lisent depuis des fichiers DuckDB différents (etl vs elt).")
                print("  → Les écarts reflètent l'état du cache OS, pas la logique des pipelines.")
                print("  → Cette métrique n'est pas déterminante pour le choix ETL vs ELT.")

            elif choice == "6":
                plot_grouped_by_scale(analysis_df, 'memory_peak',
                    "Pic memoire (MB)",
                    "Phase Analyse — Pic memoire par requete et par scale : ETL vs ELT")
                print("\n📊 Interprétation :")
                print("  → Même observation que le temps d'exécution — les différences sont dues au cache.")
                print("  → Pour comparer la mémoire analytique de façon équitable, il faudrait")
                print("    les deux pipelines sur le même fichier DuckDB.")

            elif choice == "7":
                print("\n  Sur quelle metrique ? \n")
                print("  a. Temps d'execution total\n")
                print("  b. Pic memoire\n")
                print("  c. Debit\n")
                sub_choice = input("  Choix (a/b/c) : \n").lower()
                while sub_choice not in ["a", "b", "c"]:
                    sub_choice = input("  Choix invalide (a/b/c) : \n").lower()
                metric_map = {
                    "a": ("execution_time", "Temps d'execution total (s)"),
                    "b": ("memory_peak",    "Pic memoire (MB)"),
                    "c": ("throughput",     "Debit (lignes/s)"),
                }
                metric, ylabel = metric_map[sub_choice]
                plot_scalability(transform_df, metric, ylabel,
                    f"Scalabilite — {ylabel} selon le scale factor : ETL vs ELT")
            print("\n📊 Interprétation :")
            print("  → Une courbe ETL plus pentue indique qu'il ne passe pas à l'échelle linéairement.")
            print("  → ELT (dbt + DuckDB) devrait rester plus stable grâce au traitement SQL columnar.")
            print("  → Si les deux courbes sont proches, les deux approches sont équivalentes")
            print("    pour votre volume de données.")


        case "5":
            import pandas as pd
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            print("\nLancement des tests de qualité des données...\n")
            print("1. Lancer les tests ETL (Pandas) et ELT (dbt) maintenant\n")
            print("2. Charger les résultats depuis les fichiers JSON existants\n")

            sub = input("Choix (1 ou 2) : \n")
            while sub not in ["1", "2"]:
                sub = input("Choix invalide (1 ou 2) : \n")

            if sub == "1":
                etl_test_results = etl_tests.launch()
                dbt_test_results = dbt.launch_tests()

                etl_df = pd.DataFrame(etl_test_results)
                dbt_df = pd.DataFrame(dbt_test_results)
                etl_df.to_json('etl_test_results.json', orient='records', indent=4)
                dbt_df.to_json('dbt_test_results.json', orient='records', indent=4)
            else:
                etl_df = pd.read_json('etl_test_results.json')
                dbt_df = pd.read_json('dbt_test_results.json')

            etl_df['type'] = 'ETL (Pandas)'
            dbt_df['type'] = 'ELT (dbt)'
            all_df    = pd.concat([etl_df, dbt_df], ignore_index=True)
            models    = list(etl_df['analysis_name'].str.replace('Tests ', ''))
            COLORS    = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}
            pipelines = ['ETL (Pandas)', 'ELT (dbt)']

            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=(
                    "Temps d'exécution des tests (s)",
                    "Tests passés vs échoués",
                )
            )

            for i, pipeline in enumerate(pipelines):
                sub_df = all_df[all_df['type'] == pipeline].reset_index(drop=True)

                fig.add_trace(go.Bar(
                    name=pipeline, x=[pipeline],
                    y=[float(sub_df['execution_time'].sum())],
                    marker_color=COLORS[pipeline],
                    text=[f"{sub_df['execution_time'].sum():.4f}s"],
                    textposition='outside',
                    showlegend=True,
                ), row=1, col=1)

                fig.add_trace(go.Bar(
                    name=f"{pipeline} - Passés",
                    x=[f"{pipeline[:3]} Passés", f"{pipeline[:3]} Échoués"],
                    y=[int(sub_df['tests_passed'].sum()), int(sub_df['tests_failed'].sum())],
                    marker_color=[COLORS[pipeline], 'crimson'],
                    showlegend=False,
                ), row=1, col=2)

            fig.update_layout(
                title_text="Comparaison Maintenabilité — Tests de qualité ETL (Pandas) vs ELT (dbt)",
                barmode='group',
                template='plotly_white',
                height=500,
                legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
            )
            fig.show()
            print("\n📊 Interprétation :")
            print("  → dbt exécute ses 29 tests SQL en parallèle (4 threads DuckDB) → souvent plus rapide.")
            print("  → ETL exécute ses tests en Python pur, sans parallélisme → plus lent sur fact_sales.")
            print("  → Les deux approches couvrent le même périmètre (29 tests identiques).")
            print("  → Avantage dbt : les tests sont déclaratifs (schema.yml), aucun code Python à écrire.")

            # Résumé texte
            for pipeline, df_p in [('ETL (Pandas)', etl_df), ('ELT (dbt)', dbt_df)]:
                tp = int(df_p['tests_passed'].sum())
                tf = int(df_p['tests_failed'].sum())
                tt = float(df_p['execution_time'].sum())
                print(f"\n{pipeline} : {tp+tf} tests | {tp} passés | {tf} échoués | "
                      f"durée totale : {tt:.4f}s | LOC tests : "
                      + ("~70 (manuel)" if 'Pandas' in pipeline else "~60 (schema.yml)"))

        case "6":
            from plotly.subplots import make_subplots

            etl_cx, elt_cx = complexity.launch()

            # Graphe comparatif
            metrics_plot = [
                ("total_sloc",  "SLOC (lignes de code source)"),
                ("avg_cc",      "CC moyen (complexité cyclomatique)"),
                ("avg_mi",      "MI moyen (maintenabilité, /100)"),
                ("n_files",     "Nombre de fichiers"),
                ("comment_ratio", "% Commentaires"),
            ]

            fig = make_subplots(
                rows=1, cols=len(metrics_plot),
                subplot_titles=[m[1] for m in metrics_plot],
            )
            COLORS = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}

            for col, (metric, label) in enumerate(metrics_plot, start=1):
                for pipeline, data in [('ETL (Pandas)', etl_cx), ('ELT (dbt)', elt_cx)]:
                    fig.add_trace(go.Bar(
                        name         = pipeline,
                        x            = [pipeline],
                        y            = [data[metric]],
                        marker_color = COLORS[pipeline],
                        text         = [f"{data[metric]:.1f}"],
                        textposition = 'outside',
                        showlegend   = (col == 1),
                        legendgroup  = pipeline,
                    ), row=1, col=col)

            fig.update_layout(
                title_text = "Complexité de développement — ETL (Pandas) vs ELT (dbt)",
                barmode    = 'group',
                template   = 'plotly_white',
                height     = 500,
                legend     = dict(orientation="h", yanchor="bottom", y=1.05,
                                  xanchor="right", x=1),
            )
            fig.show()
            print("\n📊 Interprétation :")
            print("  → ETL a plus de SLOC car l'approche impérative (Pandas) exige du code")
            print("    explicite pour chaque transformation : boucles, merges, renommages.")
            print("  → ELT a un CC plus bas : le SQL déclaratif n'a pas de branches if/else.")
            print("  → Le MI d'ELT est meilleur car les fichiers dbt (SQL + YAML) sont courts")
            print("    et ont une responsabilité unique (un fichier = un modèle).")
            print("  → Conclusion : dbt est plus facile à maintenir et à faire évoluer.")

        case "7":
            from plotly.subplots import make_subplots

            etl_lg, elt_lg = lineage.launch()

            # Graphe comparatif — métriques numériques
            metrics_lg = [
                ("n_nodes",              "Nœuds DAG"),
                ("n_edges",              "Arêtes (dépendances)"),
                ("lineage_depth",        "Profondeur lignage"),
                ("doc_coverage_pct",     "Docs modèles (%)"),
                ("col_doc_coverage_pct", "Docs colonnes (%)"),
                ("n_tests",              "Tests qualité"),
            ]

            fig = make_subplots(
                rows=2, cols=3,
                subplot_titles=[m[1] for m in metrics_lg],
            )
            COLORS = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}

            for idx, (metric, label) in enumerate(metrics_lg):
                row = idx // 3 + 1
                col = idx %  3 + 1
                for pipeline, data in [('ETL (Pandas)', etl_lg), ('ELT (dbt)', elt_lg)]:
                    fig.add_trace(go.Bar(
                        name         = pipeline,
                        x            = [pipeline],
                        y            = [data[metric]],
                        marker_color = COLORS[pipeline],
                        text         = [f"{data[metric]:.1f}"],
                        textposition = 'outside',
                        showlegend   = (idx == 0),
                        legendgroup  = pipeline,
                    ), row=row, col=col)

            fig.update_layout(
                title_text = "Data Lineage & Documentation — ETL (Pandas) vs ELT (dbt)",
                barmode    = 'group',
                template   = 'plotly_white',
                height     = 600,
                legend     = dict(orientation="h", yanchor="bottom", y=1.02,
                                  xanchor="right", x=1),
            )
            fig.show()
            print("\n📊 Interprétation :")
            print("  → dbt génère le lineage automatiquement depuis manifest.json :")
            print("    chaque modèle connaît ses sources et ses dépendants (DAG complet).")
            print("  → ETL : le lineage est implicite dans le code Python — il faut lire")
            print("    le code pour comprendre qui dépend de qui.")
            print("  → La couverture docs colonnes est 0% pour ETL : impossible nativement.")
            print("  → dbt docs generate produit un site web navigable avec le DAG interactif.")
            print("  → Pour un projet en équipe, dbt offre une traçabilité incomparable.")

        case "8":
            print("Merci d'avoir utilisé le projet ETL vs DBT. À bientôt !")
            exit(0)