/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.lang.StringBuilder;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {

  public static boolean isBlank(String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if ((Character.isWhitespace(str.charAt(i)) == false)) {
          return false;
        }
      }
      return true;
  }

  /**
   * Generate a String by appending all the @{elements}, converted to Strings, delimited by
   * @{delim}.
   * @param elements list of elements to concatenate
   * @param delim delimiter to place between each element
   * @return the concatenated string with delimiters
   */
  public static <T> String join(Iterable<T> elements, String delim) {
    StringBuilder result = new StringBuilder();
    boolean first = true;
    for (T elem : elements) {
      if (first) {
        first = false;
      } else {
        result.append(delim);
      }
      result.append(elem);
    }
    return result.toString();
  }

  /**
   * @param str
   * @param delim
   * @return extract the left part of the @str String from the last occurrence of @delim
   */
  public static String left(String str, String delim) {
    return (str.lastIndexOf(delim) > -1 ) ? str.substring(0, str.lastIndexOf(delim)): str;
  }

  /**
   * @param str
   * @param delim
   * @return extract the right part of the @str String from the first occurrence of @delim
   */
  public static String right(String str, String delim) {
    return (str.indexOf(delim) > -1 ) ? str.substring(str.indexOf(delim)+1): str;
  }

  public static String toSnakeCase(String str) {
    String camelOrPascalCase = str;
    if (camelOrPascalCase == null || camelOrPascalCase.isEmpty()) {
      return camelOrPascalCase;
    }

    StringBuilder builder = new StringBuilder();
    boolean uppercaseGroupStarted = true;
    int max = camelOrPascalCase.length();
    builder.append(Character.toLowerCase(camelOrPascalCase.charAt(0)));
    for (int i = 1; i < max; i++) {
      if (camelOrPascalCase.charAt(i) != '.') {
        if (Character.isUpperCase(camelOrPascalCase.charAt(i))) {
          if (!uppercaseGroupStarted || (i < max-2 && Character.isLowerCase(camelOrPascalCase.charAt(i+1)))) {
            builder.append("_");
            uppercaseGroupStarted = true;
          }
          builder.append(Character.toLowerCase(camelOrPascalCase.charAt(i)));
        } else {
          uppercaseGroupStarted = false;
          builder.append(camelOrPascalCase.charAt(i));
        }
      }
      else {
        builder.append(camelOrPascalCase.charAt(i));
        if (i<max-1) {
          builder.append(Character.toLowerCase(camelOrPascalCase.charAt(++i)));
          uppercaseGroupStarted = true;
        }
      }
    }

    return builder.toString();
  }
}