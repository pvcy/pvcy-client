# Copyright 2023 Privacy Dynamics, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ruamel.yaml import YAML


def get_yaml_serializer() -> YAML:
    serializer = YAML(typ="rt")
    serializer.default_flow_style = False
    serializer.sort_base_mapping_type_on_output = False  # type: ignore
    serializer.indent(mapping=2, sequence=4, offset=2)
    return serializer
