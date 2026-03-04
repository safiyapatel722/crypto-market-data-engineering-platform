import argparse
from src.pipelines.backfill_pipeline import BackfillPipeline

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    args = parser.parse_args()

    if args.mode == "backfill":
        pipeline = BackfillPipeline()
        pipeline.run()
    else:
        print("Invalid mode")

if __name__ == "__main__":
    main()