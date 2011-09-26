## Storm-Kafka

A beautiful marriage between [Storm](https://github.com/nathanmarz/storm) and [Kafka](http://incubator.apache.org/kafka). Seriously though, this library will let you use Kafka as a spout within Storm. It is based on the [storm-kestrel](https://github.com/nathanmarz/storm-kestrel) spout but in the end, don't share too much in common.

### Notes

See the javadoc of the KafkaSpout for details. The biggest thing to note is that the current implementation does not guarantee reliability in the Storm topology.

### License

<pre>
Copyright 2011 Josh Devins

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
</pre>