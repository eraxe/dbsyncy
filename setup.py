from setuptools import setup, find_packages

setup(
    name='dbsyncy',
    version='0.1',
    packages=find_packages(),
    scripts=['scripts/main.py'],
    install_requires=[
        'mysql-connector-python',
        'tqdm',
        'colorama',
        'termcolor'
    ],
    entry_points={
        'console_scripts': [
            'dbsyncy=scripts.main:main',
        ],
    },
    author='Arash Abolhasani',
    author_email='parppl@gmail.com',
    description='A tool for synchronizing databases efficiently.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='http://github.com/eraxe',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
