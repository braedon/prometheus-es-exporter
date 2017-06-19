from setuptools import setup, find_packages

setup(
    name='prometheus-es-exporter',
    version='0.4.0',
    description='Elasticsearch query Prometheus exporter',
    url='https://github.com/Braedon/prometheus-es-exporter',
    author='Braedon Vickers',
    author_email='braedon.vickers@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Monitoring',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
    keywords='monitoring prometheus exporter elasticsearch',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'elasticsearch',
        'jog',
        'prometheus-client'
    ],
    entry_points={
        'console_scripts': [
            'prometheus-es-exporter=prometheus_es_exporter:main',
        ],
    },
)
