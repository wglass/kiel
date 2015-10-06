from setuptools import setup, find_packages

from kiel import __version__


setup(
    name="kiel",
    version=__version__,
    description="Kafka client for tornado async applications.",
    author="William Glass",
    author_email="william.glass@gmail.com",
    url="http://github.com/wglass/kiel",
    license="Apache",
    keywords=["kafka", "tornado", "async"],
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
        "tornado>=4.1",
        "kazoo",
        "six",
    ],
    extras_require={
        "snappy": [
            "python-snappy"
        ]
    },
    tests_require=[
        "nose",
        "mock",
        "coverage",
        "flake8",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
