package com.github.dllen.addax;

import com.github.dllen.utils.FreePortFinder;
import java.security.PrivilegedAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;


public class AddaxExecutor {

    private static final Log LOG = LogFactory.getLog(AddaxExecutor.class);

    public static void run(String[] args) {
        String masterAddr = args[0];
        String containerId = args[1];
        int port = FreePortFinder.findFreeLocalPort(9090);
        AddaxExecutorHttpServer addaxExecutorHttpServer = new AddaxExecutorHttpServer(masterAddr, port, containerId);
        addaxExecutorHttpServer.start();
    }

    public static String getCurrentUserName() throws Exception {
        // Get user from container environment
        String user = System.getenv("USER");
        if (user == null || user.isEmpty()) {
            LOG.info("Environment USER is not set, use user from UserGroupInformation.");
            user = UserGroupInformation.getCurrentUser().getShortUserName();
        }
        return user;
    }

    public static void transferCredentials(UserGroupInformation source, UserGroupInformation dest) {
        for (Token<? extends TokenIdentifier> tokenIdentifier : source.getTokens()) {
            dest.addToken(tokenIdentifier);
        }
    }

    public static void main(final String[] args) throws Exception {
        String user = getCurrentUserName();
        LOG.info("Run Server as user:" + user);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        transferCredentials(UserGroupInformation.getCurrentUser(), ugi);
        ugi.doAs((PrivilegedAction<Void>) () -> {
            try {
                run(args);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            return null;
        });
    }
}
