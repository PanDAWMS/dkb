"""
Common library for Data Knowledge Base Dataflow stages development.

Dataflow
  ETL process (extract-transform-load) for populating internal DKB
  storages and keeping them up to date

Dataflow stage
  Logical step of ETL process, implemented as standalone executable
  program (worker)

Dataflow stages are standalone programs, but can be combined into a
pipeline by means of Kafka-based supervising program. For details about
program compatibility with the supervisor please check documentation
for the Metadata Integration Topology Management System (MInT MS)
workers [#]_. Worker program can be written in any language; ``pyDKB``
is intended to simplify this process for Python.

.. warning::
  There are three types of stages corresponding three types of ETL
  operations:  *source connector* (data extraction), *processor*
  (transformation) and *sink connector* (load to internal DKB storage).
  Currently ``pyDKB`` library can be used only for *processor* stages, but
  in future versions *connector* stages will also be supported.

Quickstart guide
----------------

To create simple processor stage application first decide input and
output data format. In following examples we will work with data in JSON
format (for the full list of supported formats check
:ref:`pyDKB.dataflow.messages` section of this documentation).

Now let`s start writing example processor ``welcome.py`` and implement
message handler -- functional part of the stage (operations to be
performed on data flow units)::

  from pyDKB.dataflow.messages import JSONMessage

  def my_process(stage, message):
      \""" Single message processing. \"""
      input_data = message.content()
      name = input_data.get('name')
      if name:
          out_data = {'message': "Welcome, %s!" % name}
          out_message = JSONMessage(out_data)
          stage.output(out_message)
      return True

Function must take two arguments: ``stage`` (stage context object) and
``message`` (input message, which should be transformed by our stage).
Message is a smallest data unit in the data flow running through the
processor, and every message is to be processed independently of
previous or following ones. ``message.content()`` and
``JSONMessage(out_data)`` statements are used to decode/encode message
to/from Python ``dict`` object.
Message, passed to the function, is taken from the input data flow; to
write new message(s) to the output data flow,
``stage.output(out_message)`` is used. It can be used as many times as
many output messages were generated (or once with the list of messages).
In our example, messages without key ``'name'`` will produce no output
messages, so ``stage.output()`` will not be called at all.
In terms of data flow it means that the input message is filtered out
and will not reach the *sink connector*.

Boolean return value of ``my_process()`` indicates if the processing was
successful or not.
If processing failed (``False`` is returned), produced output messages
will be dropped to avoid loading sketchy information into the DKB
storages.

Now as we have processing logic implemented, we need to turn it into
fully functional application. Add following lines to ``welcome.py``:

.. code-block:: python
  :emphasize-lines: 1-2,8-

  import sys
  from pyDKB.dataflow.stage import JSONProcessorStage
  from pyDKB.dataflow.messages import JSONMessage

  def my_process(stage, message):
      <...function code...>

  if __name__ == '__main__':
      stage = JSONProcessorStage()
      stage.process = my_process
      stage.parse_args(sys.argv[1:])
      stage.run()

First we create stage object and indicate that input and output message
format is JSON: ``stage = JSONProcessorStage()`` (for full list of
processors check :ref:`pyDKB.dataflow.stage` section of this documentation);
then set stage processing function to our function ``my_process()``,
parse command line arguments (``stage.parse_args(sys.argv[1:])``) and
start the stage execution.

Easy, right?

It`s time to run our application. Create input data sample
``input.ndjson`` with following lines::

  {"name": "James", "city": "New York"}
  {"user": "Jonathan", "role": "support"}
  {"name": "John Smith"}

and type::

  $ python welcome.py --dest s input.ndjson
  {"message": "Welcome, James!"}
  {"message": "Welcome, John Smith!"}

``--dest s`` indicates that output destination is (s)tdout (default
destination is file).
For full information about modes in which the stage application can be
used, run ``python welcome.py -h``.

That`s it, your first application is ready to be integrated into an ETL
process as data processing node.
For details about ETL process creation check `MInT Supervisor` [#]_
documentation.

.. [#] WIP
.. [#] WIP

"""

import dataflow
import common

__version__ = "0.3-SNAPSHOT"

__all__ = ["dataflow"]
