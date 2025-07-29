import subprocess
import json
import os

# Intergration Tests for the main.py module in the app package
# example usage: pytest tests/test_integration.py

current_dir = os.path.dirname(os.path.abspath(__file__))
args_file_path = os.path.join(current_dir, 'args.json')


def test_with_url_param():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.path.join(current_dir, "..")) 
    result = subprocess.run(
        ["python3", os.path.join(current_dir, "../app/main.py"),
         "--url", "https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0"],
        capture_output=True,
        text=True,
        env=env
    )

    assert result.returncode == 0, f"Script failed:\n{result.stderr}"

    output_dir = './output'
    files = os.listdir(output_dir)
    latest_file = max(files, key=lambda f: os.path.getmtime(
        os.path.join(output_dir, f)))

    output_file_path = os.path.join(output_dir, latest_file)
    with open(output_file_path, 'r') as f:
        output = json.load(f)

    assert "summary" in output
    assert isinstance(output["products"], list)
    assert len(output["products"]) == 100  # Assuming the API returns 100 posts


def test_with_file_args():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.path.join(current_dir, "..")) 
    result = subprocess.run(
        ["python3", os.path.join(
            current_dir, "../app/main.py"), "--args_file", args_file_path],
        capture_output=True,
        text=True,
        env=env
    )

    assert result.returncode == 0, f"Script failed:\n{result.stderr}"

    output_dir = './output'
    files = os.listdir(output_dir)
    latest_file = max(files, key=lambda f: os.path.getmtime(
        os.path.join(output_dir, f)))

    output_file_path = os.path.join(output_dir, latest_file)

    with open(output_file_path, 'r') as f:
        output = json.load(f)

    assert "summary" in output
    assert isinstance(output["products"], list)

# test depulication of endpoints in both parameters
# endpoint https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0 is a duplicate in args.json
def test_with_both_params():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.path.join(current_dir, "..")) 
    result = subprocess.run(
        ["python3", os.path.join(current_dir, "../app/main.py"), "--url",
         "https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0", "--args_file", args_file_path],
        capture_output=True,
        text=True,
        env=env
    )

    assert result.returncode == 0, f"Script failed:\n{result.stderr}"

    output_dir = './output'
    files = os.listdir(output_dir)
    latest_file = max(files, key=lambda f: os.path.getmtime(
        os.path.join(output_dir, f)))

    output_file_path = os.path.join(output_dir, latest_file)

    with open(output_file_path, 'r') as f:
        output = json.load(f)

    assert "summary" in output
    assert isinstance(output["products"], list)
    # Assuming the url and args.json have 4 endpoints
    # But I have deduplicated them in the main.py script, so the total is 3 endpoints and 306 products
    assert len(output["products"]) == 306

# Test with invalid URL format
def test_with_invalid_url():
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.abspath(os.path.join(current_dir, "..")) 
    result = subprocess.run(
        ["python3", os.path.join(
            current_dir, "../app/main.py"), "--url", "https://lll.com|invalid|0"],
        capture_output=True,
        text=True,
        env=env
    )

    assert "Error fetching data from https://lll.com" in result.stderr, f"Expected error message not found in stderr:\n{result.stderr}"
