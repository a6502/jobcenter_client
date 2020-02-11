import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jobcenter-client-a6502",
    version="0.0.1",
    author="Wieger Opmeer",
    author_email="pypi@a6502.net",
    description="A JobCenter Client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/a6502/jobcenter_client",
    packages=setuptools.find_packages(),
    license='MIT License',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=['pynetstring'],
)
