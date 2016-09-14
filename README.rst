******
roisto
******

roisto polls stop predictions from PubTrans databases and publishes them in Jore format via an MQTT broker.

PubTrans is a product by Hogia.
Jore is a public transit database used internally at HSL.


Dependencies
------------

pymssql requires `FreeTDS <http://www.freetds.org/>`_ to be installed on the system.


Install
-------

.. code-block:: sh

    git clone https://github.com/hsldevcom/roisto
    cd roisto
    cp config.yaml.template config.yaml
    # Change endpoints, credentials etc.
    vim config.yaml
    ./setup.py install


Run
---

.. code-block:: sh

    roisto -c config.yaml


Develop
-------

Install development dependencies:

.. code-block:: sh

    pip install -r requirements/dev.txt

Upgrade dependencies:

.. code-block:: sh

    ./upgrade_dependencies.sh
    pip-sync requirements/dev.txt


License
-------

roisto is licensed under the AGPLv3 license.
See the files LICENSE and LICENSE_AGPL for more information.
