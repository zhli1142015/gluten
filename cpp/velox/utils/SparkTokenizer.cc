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

#include "utils/SparkTokenizer.h"

namespace gluten {

SparkTokenizer::SparkTokenizer(const std::string& path) : path_(path) {
  state = State::kNotReady;
  index_ = 0;
}

bool SparkTokenizer::hasNext() {
  switch (state) {
    case State::kDone:
      return false;
    case State::kReady:
      return true;
    case State::kNotReady:
      break;
    case State::kFailed:
      VELOX_FAIL("Illegal state");
  }
  return tryToComputeNext();
}

std::unique_ptr<Subfield::PathElement> SparkTokenizer::next() {
  if (!hasNext()) {
    VELOX_FAIL("No more tokens");
  }
  state = State::kNotReady;
  return std::move(next_);
}


bool SparkTokenizer::hasNextCharacter() {
  return index_ < path_.length();
}

std::unique_ptr<Subfield::PathElement> SparkTokenizer::computeNext() {
  if (!hasNextCharacter()) {
    state = State::kDone;
    return nullptr;
  }

  std::unique_ptr<Subfield::PathElement> token = matchPathSegment();
  return token;
}

void SparkTokenizer::nextCharacter() {
  index_++;
}

std::unique_ptr<Subfield::PathElement> SparkTokenizer::matchPathSegment() {
  int start = index_;
  while (hasNextCharacter()) {
    nextCharacter();
  }
  int end = index_;

  std::string token = path_.substr(start, end - start);

  if (token.empty()) {
    invalidSubfieldPath();
  }

  return std::make_unique<Subfield::NestedField>(token);
}

void SparkTokenizer::invalidSubfieldPath() {
  VELOX_FAIL("Invalid subfield path: {}", path_);
}

bool SparkTokenizer::tryToComputeNext() {
  state = State::kFailed; // temporary pessimism
  next_ = computeNext();
  if (state != State::kDone) {
    state = State::kReady;
    return true;
  }
  return false;
}

} // namespace gluten
