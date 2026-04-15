from dbt_project import dbt
import scripts.dbgen as dbgen
import etl_project.etl as etl
import etl_project.etl_tests as etl_tests
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
    print("6. Quitter\n")
    print("===================================================\n")

    choice = input("Veuillez entrer votre choix (1, 2, 3, 4, 5 ou 6) : \n")

    while choice not in ["1", "2", "3", "4", "5", "6"]:
        choice = input("Choix invalide. Veuillez entrer 1, 2, 3, 4, 5 ou 6 : \n")

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
            dbt_results = pd.read_json('dbt_benchmark_results.json')
            etl_results = pd.read_json('etl_benchmark_results.json')

            dbt_results['type'] = 'ELT (dbt)'
            etl_results['type'] = 'ETL (Pandas)'

            all_results = pd.concat([dbt_results, etl_results], ignore_index=True)

            # Séparer phase Transform vs phase Analyse
            transform_mask = all_results['analysis_name'].str.startswith('Transform')
            transform_df   = all_results[transform_mask].copy()
            analysis_df    = all_results[~transform_mask].copy()

            print("\n== Que voulez-vous comparer ? ==\n")
            print("1. Temps execution  — phase Transformation (par table)\n")
            print("2. Temps CPU        — phase Transformation (par table)\n")
            print("3. Debit            — phase Transformation (lignes/seconde)\n")
            print("4. Pic memoire      — phase Transformation (par table)\n")
            print("5. Temps execution  — phase Analyse (par requete)\n")
            print("6. Pic memoire      — phase Analyse (par requete)\n")

            choice = input("Veuillez entrer votre choix (1 a 6) : \n")
            while choice not in ["1", "2", "3", "4", "5", "6"]:
                choice = input("Choix invalide. Veuillez entrer un chiffre entre 1 et 6 : \n")

            COLORS = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}

            def plot_grouped(df, metric, ylabel, title):
                tables = list(df['analysis_name'].unique())
                types  = ['ETL (Pandas)', 'ELT (dbt)']
                fig    = go.Figure()

                for pipeline in types:
                    subset = df[df['type'] == pipeline].set_index('analysis_name')
                    values = [float(subset.loc[t, metric]) if t in subset.index else 0 for t in tables]
                    fig.add_trace(go.Bar(
                        name        = pipeline,
                        x           = tables,
                        y           = values,
                        marker_color= COLORS[pipeline],
                        text        = [f"{v:,.4f}" for v in values],
                        textposition= 'outside',
                    ))

                fig.update_layout(
                    title        = title,
                    xaxis_title  = "Table / Requête",
                    yaxis_title  = ylabel,
                    barmode      = 'group',
                    xaxis_tickangle = -35,
                    legend       = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    hoverlabel   = dict(bgcolor="white", font_size=13),
                    template     = "plotly_white",
                    height       = 550,
                )
                fig.show()

            if choice == "1":
                plot_grouped(transform_df, 'execution_time', "Temps d'execution (s)",
                    "Phase Transform — Temps d'execution par table : ETL vs ELT")
            elif choice == "2":
                plot_grouped(transform_df, 'cpu_time', "Temps CPU (s)",
                    "Phase Transform — Temps CPU par table : ETL vs ELT")
            elif choice == "3":
                plot_grouped(transform_df[transform_df['throughput'].notna()], 'throughput',
                    "Lignes / seconde",
                    "Phase Transform — Debit par table : ETL vs ELT")
            elif choice == "4":
                plot_grouped(transform_df, 'memory_peak', "Pic memoire (MB)",
                    "Phase Transform — Pic memoire par table : ETL vs ELT")
            elif choice == "5":
                plot_grouped(analysis_df, 'execution_time', "Temps d'execution (s)",
                    "Phase Analyse — Temps d'execution par requete : ETL vs ELT")
            elif choice == "6":
                plot_grouped(analysis_df, 'memory_peak', "Pic memoire (MB)",
                    "Phase Analyse — Pic memoire par requete : ETL vs ELT")

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
            all_df = pd.concat([etl_df, dbt_df], ignore_index=True)

            models    = list(etl_df['analysis_name'].str.replace('Tests ', ''))
            COLORS    = {'ETL (Pandas)': 'orange', 'ELT (dbt)': 'steelblue'}
            pipelines = ['ETL (Pandas)', 'ELT (dbt)']

            fig = make_subplots(
                rows=1, cols=3,
                subplot_titles=(
                    "Temps d'exécution des tests (s)",
                    "Nb tests par suite",
                    "Tests passés vs échoués"
                )
            )

            import numpy as np
            x = np.arange(len(models))
            width = 0.35

            for i, pipeline in enumerate(pipelines):
                sub_df = all_df[all_df['type'] == pipeline].reset_index(drop=True)

                # Graphe 1 — Temps d'exécution
                fig.add_trace(go.Bar(
                    name=pipeline, x=models,
                    y=sub_df['execution_time'].tolist(),
                    marker_color=COLORS[pipeline],
                    text=[f"{v:.4f}s" for v in sub_df['execution_time']],
                    textposition='outside',
                    showlegend=(i == 0),
                    legendgroup=pipeline,
                ), row=1, col=1)

                # Graphe 2 — Nombre de tests
                total_tests = (sub_df['tests_passed'] + sub_df['tests_failed']).tolist()
                fig.add_trace(go.Bar(
                    name=pipeline, x=models,
                    y=total_tests,
                    marker_color=COLORS[pipeline],
                    text=total_tests,
                    textposition='outside',
                    showlegend=False,
                    legendgroup=pipeline,
                ), row=1, col=2)

                # Graphe 3 — Passés vs Échoués (stacké)
                fig.add_trace(go.Bar(
                    name=f"{pipeline} - Passés",
                    x=[f"{m}<br>({pipeline.split()[0]})" for m in models],
                    y=sub_df['tests_passed'].tolist(),
                    marker_color=COLORS[pipeline],
                    opacity=0.9,
                    showlegend=False,
                ), row=1, col=3)
                fig.add_trace(go.Bar(
                    name=f"{pipeline} - Échoués",
                    x=[f"{m}<br>({pipeline.split()[0]})" for m in models],
                    y=sub_df['tests_failed'].tolist(),
                    marker_color='crimson',
                    opacity=0.7,
                    showlegend=False,
                ), row=1, col=3)

            fig.update_layout(
                title_text="Comparaison Maintenabilité — Tests de qualité ETL (Pandas) vs ELT (dbt)",
                barmode='group',
                template='plotly_white',
                height=550,
                legend=dict(orientation="h", yanchor="bottom", y=1.05,
                            xanchor="right", x=1),
            )
            fig.update_traces(row=1, col=3)
            fig.update_layout(barmode='stack')
            fig.show()

            # Résumé texte
            for pipeline, df in [('ETL (Pandas)', etl_df), ('ELT (dbt)', dbt_df)]:
                tp = df['tests_passed'].sum()
                tf = df['tests_failed'].sum()
                tt = df['execution_time'].sum()
                print(f"\n{pipeline} : {tp+tf} tests | {tp} passés | {tf} échoués | "
                      f"durée totale : {tt:.4f}s | LOC tests : "
                      + ("~70 (manuel)" if 'Pandas' in pipeline else "~60 (schema.yml)"))

        case "6":
            print("Merci d'avoir utilisé le projet ETL vs DBT. À bientôt !")
            exit(0)