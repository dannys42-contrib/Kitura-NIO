import Foundation
import Dispatch
import NIO
import XCTest
import KituraNet
import NIOHTTP1
import NIOWebSocket
import LoggerAPI

class ConnectionLimitTests: KituraNetTest {
    static var allTests: [(String, (ConnectionLimitTests) -> () throws -> Void)] {
        return [
            ("testConnectionLimit", testConnectionLimit),
        ]
    }
    
    override func setUp() {
        doSetUp()
    }

    override func tearDown() {
        doTearDown()
    }
    private func sendRequest(request: HTTPRequestHead, on channel: Channel) {
        channel.write(NIOAny(HTTPClientRequestPart.head(request)), promise: nil)
        try! channel.writeAndFlush(NIOAny(HTTPClientRequestPart.end(nil))).wait()
    }
    
    func establishConnection(responseHandler: HTTPResponseHandler) throws -> Channel {
        var channel: Channel
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let bootstrap = ClientBootstrap(group: group)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelInitializer { channel in
                channel.pipeline.addHTTPClientHandlers().flatMap {_ in
                    channel.pipeline.addHandler(responseHandler)
                }
        }
        try channel = bootstrap.connect(host: "localhost", port: self.port).wait()
        let request = HTTPRequestHead(version: HTTPVersion(major: 1, minor: 1), method: .GET, uri: "/")
        self.sendRequest(request: request, on: channel)

        return channel
    }

    func testConnectionLimit() {
        let delegate = TestConnectionLimitDelegate()
        performServerTest(serverConfig: ServerOptions(requestSizeLimit: 10000, connectionLimit: 1), delegate, socketType: .tcp, useSSL: false, asyncTasks: { expectation in
        let payload = "Hello, World!"
        var payloadBuffer = ByteBufferAllocator().buffer(capacity: 1024)
        payloadBuffer.writeString(payload)
        _ = try self.establishConnection(responseHandler: HTTPResponseHandler(expectation: expectation, expectedStatus:HTTPResponseStatus.ok))
    }, { expectation in
        let payload = "Hello, World!"
        var payloadBuffer = ByteBufferAllocator().buffer(capacity: 1024)
        payloadBuffer.writeString(payload)
        _ = try self.establishConnection(responseHandler: HTTPResponseHandler(expectation: expectation, expectedStatus:HTTPResponseStatus.serviceUnavailable))
        })
    }

    func testThat_ConnectionIsSuccessful_AfterLimitReached() {
        let delegate = TestConnectionLimitDelegate()
        let maxConnections = 1
        let numClogClients = 5
        let numCheckClients = 10
        let customResponse: (Int, String) -> (HTTPStatusCode, String)? = { limit, client in
            return (.badRequest, "Too many connections (more than \(limit))")
        }
        var clogClientsExpectations: [XCTestExpectation] = []
        var clogClientsStatus: [HTTPResponseStatus] = []
        for i in 0..<numClogClients {
            clogClientsExpectations.append( self.expectation(description: "CLOG Client \(i)") )
            clogClientsStatus.append(HTTPResponseStatus.imATeapot)
        }
        var checkClientsStatus: [HTTPResponseStatus] = []
        var checkClientsExpectations: [XCTestExpectation] = []
        for i in 0..<numCheckClients {
            checkClientsExpectations.append( self.expectation(description: "CHECK Client \(i)") )
            checkClientsStatus.append(.imATeapot)
        }

        let serverExpectation = self.expectation(description: "server")

        performServerTest(serverConfig: ServerOptions(requestSizeLimit: 10000, connectionLimit: 1), delegate, socketType: .tcp, useSSL: false, asyncTasks: { expectation in
            defer {
                expectation.fulfill()
                serverExpectation.fulfill()
            }

            for i in 0..<numClogClients {
                let expect = clogClientsExpectations[i]
                _ = try self.establishConnection(responseHandler: HTTPResponseHandler(handler: { (status) in
                    clogClientsStatus[i] = status
                }, expectation: expect) )
            }

            self.wait(for: clogClientsExpectations, timeout: 5)

            for i in 0..<numCheckClients {
                let expect = checkClientsExpectations[i]
                _ = try self.establishConnection(responseHandler: HTTPResponseHandler(handler: { (status) in
                    checkClientsStatus[i] = status
                }, expectation: expect) )

                self.wait(for: [expect], timeout: 3)
            }
        })

        self.wait(for: [serverExpectation], timeout: 10) { error in
            XCTAssertNil(error, "Timeout")
        }

        let clogSuccessCount = clogClientsStatus.filter { $0 == .ok }.count
        let clogFailureCount = clogClientsStatus.filter { $0 != .ok }.count
        XCTAssertEqual(clogSuccessCount, maxConnections, "MaxClients should be successful")
        XCTAssertEqual(clogFailureCount, numClogClients-maxConnections, "Remaining should be failure")

        let checkSuccessCount = checkClientsStatus.filter { $0 == .ok }.count
        XCTAssertEqual(checkSuccessCount, numCheckClients, "MaxClients should be successful")

    }

}

class TestConnectionLimitDelegate: ServerDelegate {
    func handle(request: ServerRequest, response: ServerResponse) {
        do {
            try response.end()
        } catch {
            XCTFail("Error while writing response")
        }
    }
}

class HTTPResponseHandler: ChannelInboundHandler {
    let expectation: XCTestExpectation
    var handler: (HTTPResponseStatus)->Void
    var didFullfillExpectation = false

    init(handler: @escaping (HTTPResponseStatus)->Void, expectation: XCTestExpectation) {
        self.handler = handler
        self.expectation = expectation
    }
    convenience init(expectation: XCTestExpectation, expectedStatus: HTTPResponseStatus) {
        self.init(handler: { status in
            XCTAssertEqual(status, expectedStatus)
        }, expectation: expectation)
    }

    typealias InboundIn = HTTPClientResponsePart
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let response = self.unwrapInboundIn(data)
        switch response {
        case .head(let header):
            let status = header.status
            self.handler(status)
            print("did fullfill response handler expectation")
            expectation.fulfill()
            self.didFullfillExpectation = true
        case.end(_):
            if !self.didFullfillExpectation {
                print("did fullfill response handler expectation (at end)")
                expectation.fulfill()
                self.didFullfillExpectation = true
            }
        default: do {
            }
        }
    }
}
