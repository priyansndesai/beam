#  Copyright 2022 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# standard libraries
import logging
import random
import time
import argparse

# third party libraries
import apache_beam as beam
from apache_beam import Create, GroupByKey, Map, DoFn, ParDo
from apache_beam import Create
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class GenerateData(beam.DoFn):
  def __init__(self):
    return
  def process(self, element, *args, **kwargs):
    for i in range(10):
      yield (element, "A" * 2000000)
class MixLatencyFn(beam.DoFn):
  def process(self, element, *args, **kwards):
    key = element[0]
    if "2" in key or "4" in key or "6" in key or "1" in key:
      for value in element[1]:
        yield (key, value)
    elif "5" in key or "7":
      count = 0
      for value in element[1]:
        if count == 0 or count == 2:
          time.sleep(60)
        count += 1
        yield (key, value)
    else:
      return

class ShuffleFn(beam.DoFn):
  def process(self, element, *args, **kwards):
    for value in element[1]:
      yield (element[0], value)
    return

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  _, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  # Define the input key-value pairs
  elements = [
      "Key0",
      "Key1",
      "Key2",
      "Key3",
      "Key4",
      "Key5",
      "Key6",
      "Key7",
      "Key8",
      "Key9"
  ]

  # Create a PCollection of elements defined above with beam.Create().
  # Use beam.GroupByKey() to group elements by their key.
  with beam.Pipeline(options=pipeline_options) as p:
    output = (
        p
        | "Create Keys" >> Create(elements)
        | "Generate Data" >> ParDo(GenerateData())
        | "Group Elements" >> GroupByKey()
        | "Iteration ParDo" >> ParDo(ShuffleFn())
    )


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()