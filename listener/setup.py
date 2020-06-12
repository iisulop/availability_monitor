import setuptools


def get_version():
    with open("listener/__init__.py") as version_file:
        for line in version_file:
            if line.startswith("__version__ = "):
                return line.split('=')[-1].strip().strip('"')


def readme() -> str:
    with open("README.md") as f:
        return f.read()


dependencies = [
    "apscheduler>=3.6",
    "kafka-python>=2.0",
    "psycopg2-binary>=2.8",
]

test_dependencies = [
    "flake8>=3.8",
    "freezegun>=0.3",
    "pylint>=2.5",
    "pytest>=5.4",
    "python-dotenv>=0.13",
    "pytest-cov",
]

setuptools.setup(
    name="is-listener",
    version=get_version(),
    author="Iiro Sulopuisto",
    author_email="iisulop@gmail.com",
    description="Minimal availability monitor listener",
    long_description=readme(),
    long_description_content_type="text/markdown",
    extras_require=dict(test=test_dependencies),
    url="https://github.com/iisulop/availability_monitor",
    install_requires=dependencies,
    packages=setuptools.find_packages(),
    license="MIT",
)
