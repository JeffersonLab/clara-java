/*
 * Copyright (C) 2015. Jefferson Lab, CLARA framework (JLAB). All Rights Reserved.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, and not-for-profit purposes,
 * without fee and without a signed licensing agreement.
 *
 * Author Vardan Gyurjyan
 * Department of Experimental Nuclear Physics, Jefferson Lab.
 *
 * IN NO EVENT SHALL JLAB BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF JLAB HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * JLAB SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE CLARA SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED
 * HEREUNDER IS PROVIDED "AS IS". JLAB HAS NO OBLIGATION TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

package org.jlab.clara.util.shell;

import org.jlab.clara.base.error.ClaraException;
import org.jlab.clara.util.ClaraUtil;
import org.jlab.clara.util.log.ClaraLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author gurjyan
 * @version 4.x
 */
public class ClaraFork {

    // local instance of the logger object
    private static ClaraLogger lg = ClaraLogger.getInstance();

    /**
     * <p>
     *     Gets stdInput, stdOutput and stdError from the
     *     shell process object.
     *     If the process is async we sleep
     *     for a second to allow process io to become ready.
     * </p>
     * @param p shell process
     * @param isSync true if process is sync
     * @return {@link ClaraStdIO} object
     * @throws ClaraException
     */
    public static ClaraStdIO analyzeShellProcess(Process p,
                                                boolean isSync)
            throws ClaraException {
        ClaraStdIO po = new ClaraStdIO();
        StringBuilder sb;
        String s;
        BufferedReader stdInput;
        BufferedReader stdError;


        try {
            if(isSync){
                // synchronous request
                // stdInput
                if(p.getInputStream()!=null){
                    stdInput = new BufferedReader(new
                            InputStreamReader(p.getInputStream()));
                    sb = new StringBuilder();
                    if(stdInput.ready()){
                        while ((s = stdInput.readLine()) != null) {
                            sb.append(s).append("\n");
                        }
                    }
                    po.setStdio(sb.toString());
                    stdInput.close();
                }

                // stdError
                if(p.getErrorStream()!=null){
                    stdError = new BufferedReader(new
                            InputStreamReader(p.getErrorStream()));
                    sb = new StringBuilder();
                    if(stdError.ready()) {
                        while ((s = stdError.readLine()) != null) {
                            sb.append(s).append("\n");
                        }
                    }
                    po.setStdErr(sb.toString());
                    stdError.close();
                }
                // set exit value
                po.setExitValue(p.waitFor());

            } else {
                // asynchronous request
                ClaraUtil.sleep(500);
                if(p.getInputStream()!=null){
                    stdInput = new BufferedReader(new
                            InputStreamReader(p.getInputStream()));
                    sb = new StringBuilder();
                    if(stdInput.ready()){
                        String ss = stdInput.readLine();
                        if((ss != null)) {
                            sb.append(ss).append("\n");
                        }
                    }
                    po.setStdio(sb.toString());
                    stdInput.close();
                }
                // stdError
                if(p.getErrorStream()!=null){
                    stdError = new BufferedReader(new
                            InputStreamReader(p.getErrorStream()));
                    sb = new StringBuilder();
                    if(stdError.ready()) {
                        String se = stdError.readLine();
                        if(se != null) {
                            sb.append(se).append("\n");
                        }
                    }
                    po.setStdErr(sb.toString());
                    stdError.close();
                }

                // 09.20.15 vg process might be permanent, hence we do not check exitValue()
            }

        } catch ( IOException |
                InterruptedException |
                IllegalThreadStateException e) {
            throw new ClaraException(e.getMessage());
        }

        return po;

    }

    /**
     *  <p>
     *      Pipes data of a process to
     *      the next process in the chain.
     * </p>
     *
     * @param processes array of processes
     * @return {@link ClaraStdIO} object reference
     * @throws ClaraException
     */
    private static ClaraStdIO fork_pipe(Process[] processes)
            throws ClaraException {

        // Start Piper between all processes
        java.lang.Process p1;
        java.lang.Process p2;
        for (int i = 0; i < processes.length; i++) {
            p1 = processes[i];

            // If there's one more process
            if (i + 1 < processes.length) {
                p2 = processes[i + 1];
                // Start piper
                new Thread(new ClaraPiper(p1.getInputStream(),
                        p2.getOutputStream())).start();
            }
        }
        Process last = processes[processes.length - 1];
        // Wait for last process in chain;
        // may throw InterruptedException
        try {
            last.waitFor();
        } catch (InterruptedException e) {
            throw new ClaraException(e.getMessage());
        }
        // Return its InputStream
        return analyzeShellProcess(last, false);
    }


    /**
     * <p>
     *     Forks an external shell process
     * </p>
     * @param cmdL list containing command name,
     *             followed by number of parameters
     * @param isSync if true thread will
     *               wait until process is completed
     * @return {@link ClaraStdIO} object
     */
    public static ClaraStdIO fork (List<String> cmdL,
                                  boolean isSync)
            throws ClaraException {
        ClaraStdIO out;
        Process p;
        try {
            p = new ProcessBuilder(cmdL).start();
            out = analyzeShellProcess(p, isSync);
        } catch (IOException | IllegalThreadStateException e) {
            throw new ClaraException(e.getMessage());
        }
        String err = out.getStdErr();
        if(err!=null && !err.equals("")){
            lg.logger.severe(err);
        }

        return out;
    }

    /**
     * <p>
     *     Overloaded {@link #fork(java.util.List, boolean)} method
     *     that takes string as an input parameter.
     *     It handles process piping ( '|' operator, unix only),
     *     multiprocessing (';' operator). In case multi-process
     *     request stdOutput object of the last process in a chain
     *     will be returned. Note: mixing '|' and ';' is not allowed.
     * </p>
     *
     * @param cmd command line string
     * @param isSync see {@link #fork(java.util.List, boolean)}
     * @return {@link ClaraStdIO} object
     */
    public static ClaraStdIO fork (String cmd,
                                  boolean isSync)
            throws ClaraException {

        ClaraStdIO out = new ClaraStdIO();

        if(cmd.contains("|")){
            StringTokenizer st2 = new StringTokenizer(cmd,"|");
            Process[] pp = new Process[st2.countTokens()];
            for(int i=0; i<=st2.countTokens();i++) {
                String p = st2.nextToken();
                ArrayList<String> l = new ArrayList<>();
                StringTokenizer st = new StringTokenizer(p.trim());
                while(st.hasMoreTokens()){
                    l.add(st.nextToken());
                }
                try {
                    pp[i] = new ProcessBuilder(l).start();
                } catch (IOException e) {
                    throw new ClaraException(e.getMessage());
                }
            }
            out = fork_pipe(pp);

        } else if(cmd.contains(";")){
            StringTokenizer st1 = new StringTokenizer(cmd,";");
            while(st1.hasMoreTokens()){
                String p = st1.nextToken();
                ArrayList<String> l = new ArrayList<>();
                StringTokenizer st = new StringTokenizer(p.trim());
                while(st.hasMoreTokens()){
                    l.add(st.nextToken());
                }
                out = fork(l,isSync);
            }

        } else {
            ArrayList<String> l = new ArrayList<>();
            StringTokenizer st = new StringTokenizer(cmd.trim());
            while(st.hasMoreTokens()){
                l.add(st.nextToken());
            }
            out = fork(l,isSync);
        }
        if(out!=null){
            String err = out.getStdErr();
            if(err!=null && !err.equals("")){
                lg.logger.severe(err);
            }
        }
        return out;
    }


}
