import setuptools


with open("README.md", "rb") as fh:
    long_description = fh.read().decode()
with open("ariesdockerd/version.py", "r") as fh:
    exec(fh.read())
    __version__: str


def packages():
    return setuptools.find_packages(include=['ariesdocker*'])


setuptools.setup(
    name="ariesdockerd",
    version=__version__,
    author="flandre.info",
    author_email="flandre@scarletx.cn",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eliphatfs/ariesdockerd",
    packages=packages(),
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='~=3.7',
    install_requires=[
        'pyjwt',
        'docker',
        'websockets',
        'aioconsole',
        'typing_extensions'
    ],
    entry_points=dict(
        console_scripts=[
            "aries=ariesdockerd.client:sync_main",
            "ariesdockerd=ariesdockerd.daemon:sync_main",
            "ariescentral=ariesdockerd.central:sync_main",
        ]
    )
)
