import setuptools

def get_version():
    with open("availability_monitor/__init__.py") as version_file:
        for line in version_file:
            if line.startswith("__version__ = "):
                return line.split('=')[-1].strip()

dependencies = [
    "requests>=2.23",
    "pytest>=5.4",
    "kafka-python>=2.0",
    "psycopg-binary>=2.8",
]

setuptools.setup(
    name="availability_monitor",
    version=get_version(),
    author="Iiro Sulopuisto",
    author_email="iisulop@gmail.com",
    description="Minimal availability monitor producer",
    url="",
    install_requires=dependencies,
    )