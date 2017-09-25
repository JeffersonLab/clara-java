/*
 *   Copyright (c) 2017.  Jefferson Lab (JLab). All rights reserved. Permission
 *   to use, copy, modify, and distribute  this software and its documentation for
 *   educational, research, and not-for-profit purposes, without fee and without a
 *   signed licensing agreement.
 *
 *   IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL
 *   INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING
 *   OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS
 *   BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY,
 *   PROVIDED HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE
 *   MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *   This software was developed under the United States Government license.
 *   For more information contact author at gurjyan@jlab.org
 *   Department of Experimental Nuclear Physics, Jefferson Lab.
 */

package org.jlab.clara.util.report;

import org.jlab.clara.base.core.ClaraConstants;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class JsonUtils {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern(ClaraConstants.DATE_FORMAT);

    private JsonUtils() { }

    public static JSONObject readJson(String resource) {
        InputStream stream = JsonUtils.class.getResourceAsStream(resource);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            String data =  reader.lines().collect(Collectors.joining("\n"));
            return new JSONObject(data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static JSONObject getContainer(JSONObject dpe, int c) {
        return dpe.getJSONArray("containers")
                  .getJSONObject(c);
    }

    public static JSONObject getService(JSONObject dpe, int c, int s) {
        return dpe.getJSONArray("containers").getJSONObject(c)
                  .getJSONArray("services").getJSONObject(s);
    }

    public static Stream<JSONObject> dpeStream(JSONObject dpeReport, String type) {
        return Stream.of(dpeReport.getJSONObject(type));
    }

    public static Stream<JSONObject> containerStream(JSONObject dpeReport, String type) {
        return containerStream(dpeReport.getJSONObject(type));
    }

    public static Stream<JSONObject> containerStream(JSONObject dpe) {
        return arrayStream(dpe, "containers");
    }

    public static Stream<JSONObject> serviceStream(JSONObject dpeReport, String type) {
        return containerStream(dpeReport, type).flatMap(JsonUtils::serviceStream);
    }

    public static Stream<JSONObject> serviceStream(JSONObject container) {
        return arrayStream(container, "services");
    }

    public static Stream<JSONObject> arrayStream(JSONObject json, String key) {
        JSONArray array = json.getJSONArray(key);
        return IntStream.range(0, array.length())
                        .mapToObj(array::getJSONObject);
    }

    public static LocalDateTime getDate(JSONObject json, String key) {
        return LocalDateTime.parse(json.getString(key), FORMATTER);
    }
}
