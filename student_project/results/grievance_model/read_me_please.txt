Theory: Grievance Model
Theory slug: grievance_model
Project root: /Users/detraviousjamaribrinkley/Documents/Development/classes/pos-6933_conflict_forecasting/student_project
Quick mode: OFF

Folder guide
------------
model_outputs/: per-model predictions, tuning tables, metrics, and feature importance.
comparison/: benchmark comparison tables and summary scorecards.
plots/: polished figures for the memo and presentation.

Topline interpretation
----------------------
Best Grievance model by average RMSE: grievance_gbm
Best overall benchmark by average RMSE: benchmark_lag123

Grievance benchmark scorecard
-----------------------------
grievance_gbm on crps_det: 0 horizon wins, average gap 0.146
grievance_rf on crps_det: 0 horizon wins, average gap 0.196
grievance_gbm on crps_prob: 0 horizon wins, average gap 0.163
grievance_rf on crps_prob: 0 horizon wins, average gap 0.182
grievance_gbm on mse: 0 horizon wins, average gap 0.544
grievance_rf on mse: 0 horizon wins, average gap 0.592
grievance_gbm on rmse: 0 horizon wins, average gap 0.344
grievance_rf on rmse: 0 horizon wins, average gap 0.367

Recommended starting files
--------------------------
comparison/comparison_all_metrics_log_dv.csv
comparison/grievance_vs_benchmark_summary.csv
comparison/grievance_model_overview.csv
plots/all_models_metrics_by_horizon.png
plots/top_feature_importance.png
