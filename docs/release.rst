
=================
Release procedure
=================

A reminder for the maintainers on how to make a new release. Releases are made
by creating a Git tag with a semantic version and pushing to GitHub.

Make sure all your changes are committed (including an entry in HISTORY.rst).
Then run:

$ git tag -s X.Y.Z -m "X.Y.Z"
$ git push
$ git push --tags

A GitHub action will deploy the new version to PyPI.
