# Databricks notebook source

archive_bronze_folder_path = "files/archive"
archive_silver_folder_path = f"{archive_bronze_folder_path}/silver"

v_archive_data_source = "file"

bronze_folder_path = "files/latest_data_from_api"
silver_folder_path = f"{bronze_folder_path}/silver"
gold_folder_path = f"{bronze_folder_path}/gold"

results_folder_path = f"{bronze_folder_path}/results"
inc_results_folder_path = f"{bronze_folder_path}/results/inc"

v_data_source = "api"

football_input_path = "/home/jovyan/code/files/input/football"

football_players_input_path = "/home/jovyan/code/files/input/football/player_stats"
football_players_output_path = "/home/jovyan/code/files/output/football/player_stats"

football_corners_input_path = "/home/jovyan/code/files/input/football/corners"
football_corners_output_path = "/home/jovyan/code/files/output/football/corners"

dp203_input_path = "/home/jovyan/code/files/input/dp203"
dp203_output_path = "/home/jovyan/code/files/output/dp203"

monster_input_path = "/home/jovyan/code/files/input/monster"
monster_output_path = "/home/jovyan/code/files/output/monster"

ml_100k_input_path = "/home/jovyan/code/files/input/ml_100k"
ml_100k_output_path = "/home/jovyan/code/files/output/ml_100k"

ad_works_input_path = "/home/jovyan/code/files/input/ad_works"
ad_works_output_path = "/home/jovyan/code/files/output/ad_works"
ad_works_data_lake = f"{ad_works_input_path}/data_lake"
ad_works_dwh_silver = f"{ad_works_output_path}/silver"
