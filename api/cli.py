import argparse
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cobloc",
        description="Run the CoBLOC backend. Needs Kafka reachable at the given broker.",
    )
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--datasets", type=str, default=None,
                        help="Folder holding your CSVs (default: ./datasets)")
    parser.add_argument("--broker", type=str, default=None,
                        help="Kafka bootstrap server (default: localhost:9092)")
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    if args.datasets:
        os.environ["COBLOC_DATASETS"] = os.path.abspath(args.datasets)
    if args.broker:
        os.environ["COBLOC_BROKER"] = args.broker

    try:
        import uvicorn
    except ImportError:
        sys.exit("uvicorn is not installed. Reinstall with: pip install cobloc")

    from api.main import datasets_dir
    folder = datasets_dir()
    if not os.path.isdir(folder):
        print(f"No datasets folder at {folder}. Create it and drop CSVs in, "
              f"or pass --datasets /path/to/folder.")
    else:
        csvs = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
        print(f"Datasets: {folder} ({len(csvs)} csv files)")

    print(f"CoBLOC backend on http://{args.host}:{args.port}")
    uvicorn.run("api.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
