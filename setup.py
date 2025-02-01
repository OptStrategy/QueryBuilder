from setuptools import setup, find_packages

setup(
    name="QueryBuilder",
    version="0.1.0",
    author='Amirhossein Adibinia',
    author_email='amir.adibinia@gmail.com',
    description="an async Query Builder for MySQL",
    # long_description=open("README.md").read(),
    # long_description_content_type="text/markdown",
    url='https://github.com/OptStrategy/QueryBuilder',
    package_dir={
            '': 'src',
    },
    packages=find_packages(where='src'),
    install_requires=[
        "aiomysql==0.2.0",
        "PyMySQL==1.1.1",
        "setuptools==75.8.0",
    ],
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Specify your actual license here
        "Operating System :: OS Independent",
    ],
)
