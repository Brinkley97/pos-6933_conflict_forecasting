Theory: Natural Resources Model
Theory slug: natural_resources_model
Project root: /Users/detraviousjamaribrinkley/Documents/Development/classes/pos-6933_conflict_forecasting/student_project
Quick mode: OFF

Folder guide
------------
model_outputs/: per-model predictions, tuning tables, metrics, and feature importance.
comparison/: benchmark comparison tables and summary scorecards.
plots/: polished figures for the memo and presentation.

Topline interpretation
----------------------
Best Natural Resources model by average RMSE: natural_resources_rf
Best overall benchmark by average RMSE: benchmark_lag123

Natural Resources benchmark scorecard
-----------------------------
natural_resources_gbm on crps_det: 0 horizon wins, average gap 0.190
natural_resources_rf on crps_det: 0 horizon wins, average gap 0.209
natural_resources_gbm on crps_prob: 0 horizon wins, average gap 0.167
natural_resources_rf on crps_prob: 0 horizon wins, average gap 0.174
natural_resources_rf on mse: 0 horizon wins, average gap 0.550
natural_resources_gbm on mse: 0 horizon wins, average gap 0.554
natural_resources_rf on rmse: 0 horizon wins, average gap 0.346
natural_resources_gbm on rmse: 0 horizon wins, average gap 0.348

Recommended starting files
--------------------------
comparison/comparison_all_metrics_log_dv.csv
comparison/natural_resources_vs_benchmark_summary.csv
comparison/natural_resources_model_overview.csv
plots/all_models_metrics_by_horizon.png
plots/top_feature_importance.png
