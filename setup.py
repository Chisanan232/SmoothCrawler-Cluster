from typing import List
import setuptools
import os


packages = ["smoothcrawler_cluster"]
here = os.path.abspath(os.path.dirname(__file__))


def get_requirements(file: str) -> List[str]:
    requirements = []
    with open(file, "r", encoding="utf-8") as r:
        r_lines = r.read().splitlines()
        for r_line in r_lines:
            if len(r_line) and r_line[0] != "#":
                requirements.append(r_line)
    return requirements


requires = get_requirements(file=os.path.join(here, "requirements", "requirements.txt"))
test_requires = get_requirements(file=os.path.join(here, "requirements", "requirements-test.txt"))


about = {}
with open(os.path.join(here, packages[0], "__pkg_info__.py"), "r", encoding="utf-8") as f:
    exec(f.read(), about)


with open("README.md", "r") as fh:
    readme = fh.read()


setuptools.setup(
    name=about["__title__"],
    version=about["__version__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=about["__url__"],
    license=about["__license__"],
    description=about["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(exclude=("docs", "test", "scripts")),
    package_dir={"smoothcrawler_cluster": packages[0]},
    py_modules=packages,
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keyword="crawler web spider cluster",
    python_requires='>=3.6',
    install_requires=requires,
    tests_require=test_requires,
    project_urls={
        "Documentation": "https://smoothcrawler-cluster.readthedocs.io",
        "Source": "https://github.com/Chisanan232/SmoothCrawler-Cluster",
    },
)
