package org.tester.model;

/** Supported step protocols; HTTP/HTTPS/REST share the Netty HTTP executor. */
public enum ProtocolType {
    HTTP,
    HTTPS,
    REST
}