**Install (tested on Python 3.6.x and HUE 4.1)**

    .. code-block:: shell

        pip install -U https://github.com/tinylambda/hueshell/zipball/master#egg=hueshell


**Configuration(~/.hue.ini)**

    .. code-block:: ini

        [hue]
        url = http://127.0.0.1/
        username = username
        password = password
        default_engine = hive

**Usage**

    .. code-block:: shell

        python -m hueshell --sql "select count(1) from testtable"

