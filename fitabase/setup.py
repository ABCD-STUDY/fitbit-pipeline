from setuptools import setup

setup(name='fitabase',
        version='0.1',
        description='Thin wrapper over Fitabase API',
        author='Simon Podhajsky',
        author_email='simon.podhajsky@sri.com',
        license='MIT',
        install_requires=['pandas', 'pycurl', 'requests'],
        tests_require=['pytest', 'vcr'],
        packages=['fitabase'],
        zip_safe=False)
