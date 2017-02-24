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

package org.jlab.clara.std.cli;

import java.util.function.Consumer;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.NullCompleter;

class Argument {

    private final String name;
    private final String description;
    private final String file;
    private final Consumer<String[]> action;

    private Completer completer = NullCompleter.INSTANCE;

    Argument(String name, String description, String file) {
        this.name = name;
        this.description = description;
        this.file = file;
        this.action = args -> { };
    }

    Argument(String name, String description, Consumer<String[]> action) {
        this.name = name;
        this.description = description;
        this.file = "";
        this.action = action;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getFile() {
        return file;
    }

    public Consumer<String[]> getAction() {
        return action;
    }

    public Completer getCompleter() {
        return completer;
    }

    public void setCompleter(Completer completer) {
        this.completer = completer;
    }
}
