package com.patnaikankit.gossip;

record Endpoint(String host, int port) {
    static Endpoint parse(String value) {
        String host;
        String portText;

        if (value.startsWith("[")) {
            int closingBracket = value.indexOf(']');
            if (closingBracket < 0 || closingBracket + 1 >= value.length()
                    || value.charAt(closingBracket + 1) != ':') {
                throw new IllegalArgumentException("IPv6 endpoints must use [address]:port format: " + value);
            }
            host = value.substring(1, closingBracket);
            portText = value.substring(closingBracket + 2);
        } else {
            int separator = value.lastIndexOf(':');
            if (separator <= 0 || value.indexOf(':') != separator) {
                throw new IllegalArgumentException("endpoints must use host:port format: " + value);
            }
            host = value.substring(0, separator);
            portText = value.substring(separator + 1);
        }

        try {
            int port = Integer.parseInt(portText);
            if (host.isBlank() || port < 1 || port > 65535) {
                throw new IllegalArgumentException("endpoint host or port is out of range: " + value);
            }
            return new Endpoint(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("endpoint port must be between 1 and 65535: " + value, e);
        }
    }

    String displayAddress() {
        return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
    }
}
