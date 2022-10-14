import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    # remove header
    header_lines = 3
    long_description = long_description.split("\n", header_lines)[header_lines]

project_urls = {
    'Documentation': 'https://keboola.github.io/python-component/interface.html',
    'Component Template project': 'https://bitbucket.org/kds_consulting_team/cookiecutter-python-component'
}

setuptools.setup(
    name="keboola.component",
    version="1.3.8",
    author="Keboola KDS Team",
    project_urls=project_urls,
    setup_requires=['pytest-runner', 'flake8'],
    tests_require=['pytest'],
    install_requires=[
        'pygelf',
        'pytz',
        'deprecated'
    ],
    author_email="support@keboola.com",
    description="General library for Python applications running in Keboola Connection environment",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/keboola/python-component",
    package_dir={'': 'src'},
    packages=['keboola.component'],
    include_package_data=True,
    zip_safe=False,
    test_suite='tests',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Education",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Development Status :: 5 - Production/Stable"
    ],
    python_requires='>=3.7'
)
