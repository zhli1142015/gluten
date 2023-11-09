/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/type/Tokenizer.h"

namespace gluten {

using namespace facebook::velox::common;

class SparkTokenizer : public Tokenizer {
 public:
  explicit SparkTokenizer(const std::string& path);

  bool hasNext() override;

  std::unique_ptr<Subfield::PathElement> next() override;

 private:
  const std::string path_;
  int index_;
  State state;
  std::unique_ptr<Subfield::PathElement> next_;

  bool hasNextCharacter();

  std::unique_ptr<Subfield::PathElement> computeNext();

  std::unique_ptr<Subfield::PathElement> matchPathSegment();

  bool tryToComputeNext();

  void invalidSubfieldPath();

  void nextCharacter();
};
} // namespace gluten
