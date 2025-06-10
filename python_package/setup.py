from setuptools import setup, find_packages

setup(
    name="python_libs",  # The name of the package
    version="0.1.0",
    author="Caylent",
    author_email="marcos.foglino@caylent.com",
    description="Utility functions for the project",
    packages=find_packages(),  # Automatically finds all packages in this directory
    install_requires=[],  # Add dependencies if needed
    package_data={},
    include_package_data=True,
    python_requires=">=3.11",
)
