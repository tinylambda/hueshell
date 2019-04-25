import setuptools


setuptools.setup(
    packages=setuptools.find_packages(exclude=["tests"]),
    package_data={"": [".hue.ini"]},
    include_package_data=True,
    install_requires=["requests"],
)
