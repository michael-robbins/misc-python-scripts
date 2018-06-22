# misc-python-scripts
Random python scripts I've developed over the years

# Precommit hooks
```
$ pip3 install pre-commit
$ pip3 install black
$ cat .pre-commit-config.yaml
repos:
-   repo: https://github.com/ambv/black
    rev: stable
    hooks:
    - id: black
      language_version: python3.6

$ pre-commit install
```
