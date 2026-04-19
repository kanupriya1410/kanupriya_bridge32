import subprocess
import sys


def run(name: str, cmd: str) -> bool:
    print(f"\n{'=' * 50}")
    print(f"Running: {name}")
    print("=" * 50)
    result = subprocess.run(cmd, shell=True)
    passed = result.returncode == 0
    status = "PASSED" if passed else "FAILED"
    print(f"Result: {status}")
    return passed


checks = [
    ("Ruff lint", "ruff check ."),
    ("Ruff format check", "ruff format --check ."),
]

failed = []
for name, cmd in checks:
    if not run(name, cmd):
        failed.append(name)

print(f"\n{'=' * 50}")
if failed:
    print(f"FAILED: {failed}")
    sys.exit(1)
else:
    print("All checks passed!")
