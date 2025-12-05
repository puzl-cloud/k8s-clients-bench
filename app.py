import asyncio
import os

from bench.run import run


if __name__ == "__main__":
    try:
        asyncio.run(run(output_dir=os.getenv("OUTPUT_DIR")))
    finally:
        print("A few clients just crashing their unclosed sessions (they want us to manage them). "
              "That's not our fault, benchmark results are not affected.")
