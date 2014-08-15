PyTestlink
==========

Minimal library for accessing a Testlink_ database. Can be used to
generate reports on tests run in a test plan, or building an interactive
dashboard. See Lordboard_ for an example.

.. _Testlink: http://testlink.org
.. _Lordboard: http://github.com/gelendir/lordboard

Usage
=====

Before doing anything you need to run the setup::

    import testlink

    #configure the database and project. 'project' refers to the name of the
    #project in testlink
    testlink.setup(host='localhost',
                   database='testlink',
                   username='dbusername',
                   password='dbpassword',
                   project='testlink project')

After that you can use the rest of the API::

    from testlink import dao, report
    
    #generate an HTML report
    report_data = dao.manual_test_report()
    html_report = report.generate_report(report_data, 'html')

License
=======

Copyright (C) 2014 Gregory Eric Sanderson. 
Project is licensed under the GPLv3. See LICENSE for full details.
