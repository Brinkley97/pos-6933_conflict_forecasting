Theory: Democracy Model
Theory slug: democracy_model
Project root: /Users/detraviousjamaribrinkley/Documents/Development/classes/pos-6933_conflict_forecasting/student_project
Quick mode: OFF

Folder guide
------------
model_outputs/: per-model predictions, tuning tables, metrics, and feature importance.
comparison/: benchmark comparison tables and summary scorecards.
plots/: polished figures for the memo and presentation.

Topline interpretation
----------------------
Best Democracy model by average RMSE: democracy_gbm
Best overall benchmark by average RMSE: benchmark_lag123

Democracy benchmark scorecard
-----------------------------
democracy_gbm on crps_det: 0 horizon wins, average gap 0.171
democracy_rf on crps_det: 0 horizon wins, average gap 0.224
democracy_gbm on crps_prob: 0 horizon wins, average gap 0.163
democracy_rf on crps_prob: 0 horizon wins, average gap 0.182
democracy_gbm on mse: 0 horizon wins, average gap 0.541
democracy_rf on mse: 0 horizon wins, average gap 0.582
democracy_gbm on rmse: 0 horizon wins, average gap 0.342
democracy_rf on rmse: 0 horizon wins, average gap 0.362

Recommended starting files
--------------------------
comparison/comparison_all_metrics_log_dv.csv
comparison/democracy_vs_benchmark_summary.csv
comparison/democracy_model_overview.csv
plots/all_models_metrics_by_horizon.png
plots/top_feature_importance.png
