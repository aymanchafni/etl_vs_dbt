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
                """
                Agrégation correcte par métrique :
                  - execution_time : somme (temps total de toutes les tables)
                  - memory_peak    : max  (pic mémoire global du processus)
                  - throughput     : recalculé = sum(row_count) / sum(execution_time)
                """
                fig = go.Figure()
                for pipeline in PIPELINES:
                    sub_raw = (df[df['type'] == pipeline]
                               .dropna(subset=['scale_factor'])
                               .sort_values('scale_factor'))

                    if metric == 'memory_peak':
                        agg = (sub_raw.groupby('scale_factor')['memory_peak']
                               .max().reset_index())
                        y_vals = agg['memory_peak'].tolist()

                    elif metric == 'throughput':
                        # Recalculer le débit total : sum(lignes) / sum(temps)
                        grp = sub_raw.dropna(subset=['row_count']).groupby('scale_factor')
                        agg = grp.apply(
                            lambda g: g['row_count'].sum() / g['execution_time'].sum()
                            if g['execution_time'].sum() > 0 else 0
                        ).reset_index(name='throughput')
                        y_vals = agg['throughput'].tolist()

                    else:  # execution_time et autres métriques additives
                        agg = (sub_raw.groupby('scale_factor')[metric]
                               .sum().reset_index())
                        y_vals = agg[metric].tolist()

                    x_vals = agg['scale_factor'].tolist()

                    fig.add_trace(go.Scatter(
                        name         = pipeline,
                        x            = x_vals,
                        y            = y_vals,
                        mode         = "lines+markers",
                        marker       = dict(size=10),
                        line         = dict(width=3),
                        marker_color = COLORS[pipeline],
                        hovertemplate= f"SF=%{{x}}<br>{ylabel}=%{{y:,.2f}}<extra>{pipeline}</extra>",
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


            elif choice == "2":
                plot_grouped_by_scale(transform_df, 'cpu_time',
                    "Temps CPU (s)",
                    "Phase Transform — Temps CPU par table et par scale : ETL vs ELT")


            elif choice == "3":
                plot_grouped_by_scale(
                    transform_df[transform_df['throughput'].notna()], 'throughput',
                    "Lignes / seconde",
                    "Phase Transform — Debit par table et par scale : ETL vs ELT")


            elif choice == "4":
                plot_grouped_by_scale(transform_df, 'memory_peak',
                    "Pic memoire (MB)",
                    "Phase Transform — Pic memoire par table et par scale : ETL vs ELT")


            elif choice == "5":
                plot_grouped_by_scale(analysis_df, 'execution_time',
                    "Temps d'execution (s)",
                    "Phase Analyse — Temps d'execution par requete et par scale : ETL vs ELT")


            elif choice == "6":
                plot_grouped_by_scale(analysis_df, 'memory_peak',
                    "Pic memoire (MB)",
                    "Phase Analyse — Pic memoire par requete et par scale : ETL vs ELT")


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


        case "8":
            print("Merci d'avoir utilisé le projet ETL vs DBT. À bientôt !")
            exit(0)