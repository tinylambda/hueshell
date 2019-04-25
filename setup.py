import setuptools


setuptools.setup(
    packages=setuptools.find_packages(exclude=["hueshell/tests"]),
    package_data={"": ["hueshell/.hue.ini"]},
    include_package_data=True,
    install_requires=["requests"],
)
