package com.github.dllen.demo;

import com.github.dllen.ApplicationMaster;
import com.github.dllen.server.BaseJettyHttpServer;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 *
 * YarnAppMasterHttpServer
 */
public class DemoMasterJettyHttpServer extends BaseJettyHttpServer {

    private final ApplicationMaster applicationMaster;

    public DemoMasterJettyHttpServer(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
    }

    /**
     * WelcomeServlet
     */
    public static class WelcomeServlet extends HttpServlet {

        private final ApplicationMaster applicationMaster;
        private final String name;
        private final long startTimeInMs;

        public WelcomeServlet(ApplicationMaster applicationMaster, String name, long startTimeInMs) {
            this.applicationMaster = applicationMaster;
            this.name = name;
            this.startTimeInMs = startTimeInMs;
        }

        @Override
        public void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            try (PrintWriter out = response.getWriter()) {
                out.println("<body>");
                out.println("<h2>" + name + "</h2>");
                out.println(String.format("<div>The server has started for %d secs</div>",
                        (System.currentTimeMillis() - startTimeInMs) / 1000));
                if (applicationMaster != null) {
                    out.println(String.format("<div>Total request container count is %d</div>", applicationMaster.numTotalContainers));
                    out.println(String.format("<div>Running container count is %d</div>", applicationMaster.getRunningContainers().size()));
                    out.println("<table border=\"1\"><tr><th>ContainerId</th><th>Container Info</th><th>Node Link</th></tr>");
                    for (Map.Entry<ContainerId, Container> e : applicationMaster.getRunningContainers().entrySet()) {
                        out.println(String.format("<tr><td>%s</td><td>%s</td><td>%s</td></tr>", e.getKey().getContainerId(),
                                "ApplicationAttemptId : " + e.getKey().getApplicationAttemptId()
                                        + ",  NodeId : " + e.getValue().getNodeId()
                                        + " ,  NodeHttpAddress : " + e.getValue().getNodeHttpAddress()
                                        + "  [mem : " + e.getValue().getResource().getMemory() + ", vcores: "
                                        + e.getValue().getResource().getVirtualCores() + "]",
                                "<a href=\"" + getNodeLinks(e.getValue().getNodeHttpAddress()) + "\">"
                                        + getNodeLinks(e.getValue().getNodeHttpAddress()) + "</a>"));
                    }
                    out.println("</table>");
                }
                out.println("</body>");
            }
        }

        private String getNodeLinks(String nodeHttpAddress) {
            return "http://" + nodeHttpAddress + "/node/application/" + applicationMaster.appAttemptID.getApplicationId();
        }
    }

    @Override
    public HttpServlet getIndexPageServlet(String name) {
        return new WelcomeServlet(applicationMaster, name, System.currentTimeMillis());
    }
}
