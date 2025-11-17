# ============================================================================
# FILE: run_pipeline.py
# ============================================================================
# !/usr/bin/env python3
import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from src.pipeline import EarthquakePipeline


def main():
    parser = argparse.ArgumentParser(description='Earthquake ELT Pipeline')
    parser.add_argument('--days', type=int, default=7, help='Lookback days (default: 7)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--ingestion-only', action='store_true', help='Ingestion only')
    parser.add_argument('--transform-only', action='store_true', help='Transformations only')
    parser.add_argument('--config', type=str, default='config/config.toml', help='Config file')
    args = parser.parse_args()

    pipeline = EarthquakePipeline(args.config)
    start_time = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
    end_time = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None

    try:
        if args.ingestion_only:
            print("\n" + "=" * 80)
            print("Running INGESTION phase only")
            print("=" * 80 + "\n")
            stats = pipeline.run_ingestion(start_time, end_time, args.days)
        elif args.transform_only:
            print("\n" + "=" * 80)
            print("Running TRANSFORMATION phase only")
            print("=" * 80 + "\n")
            stats = pipeline.run_transformations()
        else:
            print("\n" + "=" * 80)
            print("Running FULL PIPELINE")
            print("=" * 80 + "\n")
            stats = pipeline.run_full_pipeline(start_time, end_time)

        print("\n" + "=" * 80)
        print("PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Status: {stats.get('status', 'unknown')}")

        if 'ingestion' in stats:
            ing = stats['ingestion']
            print(f"\nIngestion:")
            print(f"  - Events fetched: {ing.get('events_fetched', 0)}")
            print(f"  - Events valid: {ing.get('events_valid', 0)}")
            print(f"  - Events invalid: {ing.get('events_invalid', 0)}")
            print(f"  - Events loaded: {ing.get('events_loaded', 0)}")

        if 'transformations' in stats:
            trans = stats['transformations']
            print(f"\nWarehouse Counts:")
            print(f"  - Raw events: {trans.get('raw_events', 0)}")
            print(f"  - Staging events: {trans.get('staging_events', 0)}")
            print(f"  - Fact events: {trans.get('fact_events', 0)}")
            print(f"  - Dim time: {trans.get('dim_time', 0)}")
            print(f"  - Dim location: {trans.get('dim_location', 0)}")
            print(f"  - Dim event type: {trans.get('dim_event_type', 0)}")

        if 'duration_seconds' in stats:
            print(f"\nDuration: {stats['duration_seconds']:.2f} seconds")
        print("=" * 80 + "\n")
        return 0
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}\n", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
