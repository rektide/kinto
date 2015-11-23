Overview
#########

.. image:: images/overview-use-cases.png
    :align: right

*Kinto* is a lightweight JSON storage service with synchronisation and sharing
abilities. It is meant to be **easy to use** and **easy to self-host**.

*Kinto* is used at Mozilla and released under the Apache v2 licence.


.. _use-cases:

Use cases
=========

- A generic Web database for frontend applications.
- Build collaborative applications with fine-grained permissions.
- Store encrypted data at a location you control.
- Synchronise application data between different devices.

.. note::

    At Mozilla, *Kinto* is used in *Firefox* and *Firefox OS* for global synchronization
    of settings and assets, as well as a first-class solution for personal data in
    browser extensions and Web apps.


Key features
============

.. |logo-synchronisation| image:: images/logo-synchronisation.svg
   :alt: https://thenounproject.com/search/?q=syncing&i=31170
   :width: 70px

.. |logo-permissions| image:: images/logo-permissions.svg
   :alt: https://thenounproject.com/search/?q=permissions&i=23303
   :width: 70px

.. |logo-multiapps| image:: images/logo-multiapps.svg
   :alt: https://thenounproject.com/search/?q=community&i=189189
   :width: 70px

.. |logo-selfhostable| image:: images/logo-selfhostable.svg
   :alt: https://thenounproject.com/search/?q=free&i=669
   :width: 70px

.. |logo-community| image:: images/logo-community.svg
   :alt: https://thenounproject.com/search/?q=community&i=189189
   :width: 70px

.. |logo-schema| image:: images/logo-jsonschema.svg
   :alt: https://thenounproject.com/search/?q=quality+control&i=170795
   :width: 70px

+---------------------------------------------+---------------------------------------+
| |logo-synchronisation|                      | |logo-permissions|                    |
| Synchronisation                             | Fined grained permissions             |
|                                             |                                       |
+---------------------------------------------+---------------------------------------+
| |logo-schema|                               | |logo-multiapps|                      |
| JSON Schema validation                      | Universal and multi clients           |
+---------------------------------------------+---------------------------------------+
| |logo-selfhostable|                         | |logo-community|                      |
| Open Source and Self-hostable               | Designed in the open                  |
+---------------------------------------------+---------------------------------------+

**Also**

- HTTP best practices
- Pluggable authentication
- :ref:`Pluggable storage, cache, and permission backends
  <configuration-backends>`
- Configuration via a INI file or environment variables
- Built-in monitoring
- Cache control

**Ecosystem**

.. |logo-offline| image:: images/logo-offline.svg
   :alt: https://thenounproject.com/search/?q=offline&i=90580
   :width: 50px

.. |logo-python| image:: images/logo-python.svg
   :alt:
   :width: 50px

.. |logo-attachment| image:: images/logo-attachment.svg
   :alt: https://thenounproject.com/search/?q=attachment&i=169265
   :width: 50px

.. |logo-livesync| image:: images/logo-livesync.svg
   :alt: https://thenounproject.com/search/?q=refresh&i=110628
   :width: 50px

.. |logo-boilerplate| image:: images/logo-react.svg
   :alt: https://commons.wikimedia.org/wiki/File:React.js_logo.svg
   :width: 50px

.. |logo-demos| image:: images/logo-demos.svg
   :alt: https://thenounproject.com/search/?q=tutorial&i=24313
   :width: 50px

+---------------------------------------------+---------------------------------------------+
| |logo-offline|                              | |logo-python|                               |
| Offline-first `JavaScript client            | :github:`Python client                      |
| <https://kintojs.readthedocs.org>`_         | <Kinto/kinto.py>`                           |
+---------------------------------------------+---------------------------------------------+
| |logo-attachment|                           | |logo-livesync|                             |
| :github:`File attachments on records        | Live :github:`Push notifications            |
| <Kinto/kinto-attachment>`                   | <leplatrem/cliquet-pusher>`                 |
+---------------------------------------------+---------------------------------------------+
| |logo-boilerplate|                          | |logo-demos|                                |
| :github:`Kinto+React boilerplate            | :github:`Demo applications                  |
| <Kinto/kinto-react-boilerplate>`            | <Kinto/kinto/wiki/App-examples>`            |
+---------------------------------------------+---------------------------------------------+

**Coming soon**

- Administration console
- Automatic service discovery
- Push notifications using `the Push API <https://developer.mozilla.org/en-US/docs/Web/API/Push_API>`_

(See `our roadmap <https://github.com/Kinto/kinto/wiki/Roadmap>`_)


.. _overview-synchronisation:

Synchronisation
===============

Bi-directionnal synchronisation of records is a very hard topic.

*Kinto* takes some shortcuts by only providing the basics for concurrency control
and polling for changes, and not trying to resolve conflicts automatically.

Basically, each object has a revision number which is guaranteed to be incremented after
each modification. *Kinto* does not keep any history.

Clients can retrieve the list of changes that occured on a collection of records
since a specified revision. *Kinto* can also use it to avoid accidental updates
of objects.

.. image:: images/overview-synchronisation.png
    :align: center

.. note::

    *Kinto* synchronisation was designed and built by the `Mozilla Firefox Sync
    <https://en.wikipedia.org/wiki/Firefox_Sync>`_ team.


.. _comparison:

Comparison with other solutions
===============================

Before we started on Yet Another Data Storage Service, we took a look at what
was already out there, with a view to extending an existing community project
(rather than reinventing the wheel). In the end, the solutions we reviewed
didn't quite solve the problems we had - notably regarding fine-grained
permission settings.

What follows is a comparison table showing how Kinto stacks up compared to some
other projects in this space.

===========================  ======  ======  ========  =======  ==============  =======
Project                      Kinto   Parse   Firebase  CouchDB  Remote-Storage  Kuzzle
---------------------------  ------  ------  --------  -------  --------------  -------
Offline-first client         ✔       ✔       ✔         ✔        ✔
Fine-grained permissions     ✔       ✔       ✔
Easy query mechanism         ✔       ✔       ✔         [#]_     [#]_            ✔
Conflict resolution          ✔       ✔       ✔         ✔        ✔ [#]_          ?
Validation                   ✔       ✔       ✔         ✔                        ?
Revision history                                         ✔
File storage                 ✔[#]_   ✔                  ✔        ✔
Batch/bulk operations        ✔       ✔                  ✔
Changes stream               ✔[#]_   ✔       ✔         ✔                        ✔
Pluggable authentication     ✔                          ✔        [#]_
Pluggable storage / cache    ✔                                    ✔              ✔
Self-hostable                ✔                          ✔        ✔              ✔
Decentralised discovery      [#]_                                 ✔
Open source                  ✔                          ✔        ✔              ✔
Language                     Python                    Erlang   Node.js [#]_    Node.js
===========================  ======  ======  ========  =======  ==============  =======

.. [#] CouchDB uses Map/Reduce as a query mechanism, which isn't easy to
       understand for newcomers.
.. [#] Remote Storage allows "ls" on a folder, but items are not sorted or
       paginated.
.. [#] Kinto uses the same mechanisms as Remote storage for conflict handling.
.. [#] File storage is available :github:`via a plugin <Kinto/kinto-attachment>`
.. [#] Live notifications are available :github:`via a plugin for Pusher
       <leplatrem/cliquet-pusher>`.
.. [#] Remote Storage supports OAuth2.0 implicit grant flow.
.. [#] Support for decentralised discovery
       `is planned <https://github.com/Kinto/kinto/issues/125>`_ but not
       implemented yet.
.. [#] Remote Storage doesn't define any default implementation (as it is
       a procol) but makes it easy to start with JavaScript and Node.js.

You can also read `a longer explanation of our choices and motivations behind the
creation of Kinto <http://www.servicedenuages.fr/en/generic-storage-ecosystem>`_
on our blog.


.. _FAQ:

FAQ
===

How does Kinto compares to CouchDB / Remote Storage?
----------------------------------------------------

Before starting to create yet another data storage service, we had a long
look to the existing solutions, to see if that would make sense to extend
the community effort rather than re-inventing the wheel.

It appeared that solutions we looked at weren't solving the problems we had,
especially regarding fine-grained permissions.

To see how Kinto compares to these solutions,
read :ref:`the comparison table <comparison>`.

Can I encrypt my data?
----------------------

Kinto server stores any data you pass to it, be it encrypted or not.
We make it easy to `use encryption in our Kinto.js client
<http://www.servicedenuages.fr/en/kinto-encryption-example>`_.

Is there a package for my Operating System?
-------------------------------------------

No, but it's a great idea. Packaging is hard and we're a small team, so if
you'd like to help us out by maintaining packages for your favourite OS,
we'd be delighted to collaborate with you!

That said, Kinto is :ref:`easy to install with pip <installation>` and
we've got `an image set up <https://hub.docker.com/r/kinto/kinto-server/>`_
on the Docker hub, too.

Why did you chose to use Python rather than X?
----------------------------------------------

We know and love `Python <python.org>`_ for its simplicity and short
learning curve, so it was an obvious choice for the development team. In
addition, the Operations team at Mozilla is comfortable with deploying and
managing Python applications in production.

However, the protocol and concepts behind Kinto don't rely on Python *per
se*, so it is possible to have other Kinto implementations using other
languages.

Is it Web Scale?
----------------

YES™. Have a look at the ``/dev/null`` backend. ;-)

Can I store files inside Kinto?
-------------------------------

Yes, for small files you could serialize binary and store them as string (base64
for example).

For bigger files, there is the `kinto-attachment plugin
<https://github.com/Kinto/kinto-attachment/>`_.


What is Cliquet? What is the difference between Cliquet and Kinto ?
-------------------------------------------------------------------

Cliquet is a toolkit for designing micro-services. Kinto is a server built
using that toolkit.

`Read more (in french) about the differences <http://www.servicedenuages.fr/pourquoi-cliquet>`_.


I am seeing an Exception error, what's wrong?
---------------------------------------------

Have a look at the :ref:`Troubleshooting section <troubleshooting>` to
see what to do.
