import os


def cwd_to_src():
    """Change current working directory to src if necessary. This allows us to
    run tests from src directory.
    """
    cwd = os.getcwd()
    test_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.abspath(os.path.join(test_dir, "..", "src"))
    if os.path.abspath(cwd) != src_dir:
        os.chdir(src_dir)


cwd_to_src()
