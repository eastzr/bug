import { connect } from 'cloudflare:sockets';


/*
 * KV 配置：
 *
 * uuid = fe0ed675-bc05-474d-99e5-28ba9edf845a
 * p    = fallback proxy，例如 example.com:443
 *
 * 注意：
 * 原始程序没有 d / s 配置，因此这里不新增
 * subscription / SOCKS5 功能。
 */


function hexToBytes(hex) {
    const bytes = new Uint8Array(16);

    for (let i = 0; i < 16; i++) {
        bytes[i] = parseInt(
            hex.substring(i * 2, i * 2 + 2),
            16
        );
    }

    return bytes;
}


/*
 * 从 KV 读取配置
 */
async function getConfig(env) {

    const uuid =
        await env.CONFIG.get("uuid");

    const proxy =
        await env.CONFIG.get("p");

    return {
        uuid: uuid ? uuid.trim() : "",
        proxy: proxy ? proxy.trim() : ""
    };
}


/*
 * 解析 KV 中的 p
 *
 * 支持：
 *
 * example.com
 * example.com:443
 * 1.2.3.4:443
 * [2001:db8::1]:443
 */
function parseProxy(proxy) {

    if (!proxy) {
        return {
            address: null,
            port: 443
        };
    }

    let address = proxy;
    let port = 443;

    /*
     * IPv6
     */
    if (proxy.startsWith("[")) {

        const endBracket =
            proxy.indexOf("]");

        if (endBracket !== -1) {

            address =
                proxy.substring(
                    1,
                    endBracket
                );

            const portPart =
                proxy.substring(
                    endBracket + 1
                );

            if (portPart.startsWith(":")) {

                const parsedPort =
                    parseInt(
                        portPart.substring(1),
                        10
                    );

                if (!Number.isNaN(parsedPort)) {
                    port = parsedPort;
                }
            }

            return {
                address,
                port
            };
        }
    }


    /*
     * IPv4 / hostname
     */
    const lastColon =
        proxy.lastIndexOf(":");

    /*
     * 只有一个冒号时，
     * 认为是 address:port
     */
    if (
        lastColon > -1 &&
        proxy.indexOf(":") === lastColon
    ) {

        const possiblePort =
            parseInt(
                proxy.substring(lastColon + 1),
                10
            );

        if (!Number.isNaN(possiblePort)) {

            address =
                proxy.substring(
                    0,
                    lastColon
                );

            port = possiblePort;
        }
    }

    return {
        address,
        port
    };
}


export default {

    async fetch(request, env, ctx) {

        const contentType =
            request.headers.get(
                "Content-Type"
            ) || "";


        /*
         * 保持原程序的请求限制
         */
        if (
            request.method !== "POST" ||
            !contentType.startsWith(
                "application/grpc"
            )
        ) {
            return new Response(
                "Not Found",
                {
                    status: 404
                }
            );
        }


        try {

            /*
             * 从 KV 获取配置
             */
            const config =
                await getConfig(env);


            /*
             * uuid 必须存在
             */
            if (!config.uuid) {

                return new Response(
                    "UUID Not Configured",
                    {
                        status: 500
                    }
                );
            }


            /*
             * 转换 UUID
             */
            const EXPECTED_UUID =
                hexToBytes(
                    config.uuid
                        .replace(/-/g, "")
                );


            /*
             * 解析 KV 中的 p
             */
            const proxy =
                parseProxy(
                    config.proxy
                );


            return await processGrpcStream(
                request,
                EXPECTED_UUID,
                proxy.address,
                proxy.port
            );

        } catch (err) {

            return new Response(
                "Internal Error",
                {
                    status: 500
                }
            );
        }
    }
};


async function processGrpcStream(
    request,
    EXPECTED_UUID,
    proxyIP,
    proxyPort
) {

    const url =
        new URL(request.url);


    /*
     * 请求 URL 中的 proxyip / ip
     * 仍然优先使用。
     */
    let proxyIpParam =
        url.searchParams.get(
            "proxyip"
        ) ||
        url.searchParams.get(
            "ip"
        );


    /*
     * 兼容原代码的路径参数
     */
    if (!proxyIpParam) {

        const pathStr =
            url.pathname;


        const pIdx =
            pathStr.indexOf(
                "proxyip="
            );


        if (pIdx !== -1) {

            proxyIpParam =
                pathStr.substring(
                    pIdx + 8
                );

        } else {

            const iIdx =
                pathStr.indexOf(
                    "ip="
                );


            if (iIdx !== -1) {

                proxyIpParam =
                    pathStr.substring(
                        iIdx + 3
                    );
            }
        }
    }


    /*
     * 如果请求中提供 proxy，
     * 覆盖 KV 的 p。
     */
    if (proxyIpParam) {

        try {

            proxyIpParam =
                decodeURIComponent(
                    proxyIpParam
                );

        } catch (e) {}


        const slashIndex =
            proxyIpParam.indexOf("/");


        if (slashIndex !== -1) {

            proxyIpParam =
                proxyIpParam.substring(
                    0,
                    slashIndex
                );
        }


        const andIndex =
            proxyIpParam.indexOf("&");


        if (andIndex !== -1) {

            proxyIpParam =
                proxyIpParam.substring(
                    0,
                    andIndex
                );
        }


        const parsed =
            parseProxy(
                proxyIpParam
            );


        if (parsed.address) {

            proxyIP =
                parsed.address;

            proxyPort =
                parsed.port;
        }
    }


    /*
     * gRPC 解包
     */
    const unwrappedStream =
        request.body.pipeThrough(
            createGrpcUnwrapper()
        );


    const reader =
        unwrappedStream.getReader();


    let buffer =
        new Uint8Array(0);

    let parsed = null;


    while (true) {

        const {
            value,
            done
        } = await reader.read();


        if (
            done &&
            buffer.byteLength === 0
        ) {

            return new Response(
                "Bad Request",
                {
                    status: 400
                }
            );
        }


        if (value) {

            const temp =
                new Uint8Array(
                    buffer.byteLength +
                    value.byteLength
                );


            temp.set(buffer);

            temp.set(
                value,
                buffer.byteLength
            );


            buffer = temp;
        }


        if (
            buffer.byteLength >= 24
        ) {

            /*
             * Version
             */
            if (buffer[0] !== 0) {

                return new Response(
                    "Bad Version",
                    {
                        status: 400
                    }
                );
            }


            /*
             * UUID
             */
            let uuidMatch = true;


            for (
                let i = 0;
                i < 16;
                i++
            ) {

                if (
                    buffer[i + 1] !==
                    EXPECTED_UUID[i]
                ) {

                    uuidMatch = false;
                    break;
                }
            }


            if (!uuidMatch) {

                return new Response(
                    "Not Found",
                    {
                        status: 404
                    }
                );
            }


            /*
             * Addon
             */
            const addonLen =
                buffer[17];


            let offset =
                18 + addonLen;


            if (
                offset <
                buffer.byteLength
            ) {

                const command =
                    buffer[offset++];


                if (
                    offset + 1 <
                    buffer.byteLength
                ) {

                    const port =
                        (
                            buffer[offset] << 8
                        ) |
                        buffer[offset + 1];


                    offset += 2;


                    if (
                        offset <
                        buffer.byteLength
                    ) {

                        const addrType =
                            buffer[offset++];


                        let address = "";
                        let valid = false;


                        /*
                         * IPv4
                         */
                        if (
                            addrType === 1 &&
                            offset + 3 <
                            buffer.byteLength
                        ) {

                            address =
                                buffer
                                    .subarray(
                                        offset,
                                        offset + 4
                                    )
                                    .join(".");


                            offset += 4;

                            valid = true;


                        /*
                         * Domain
                         */
                        } else if (
                            addrType === 2 &&
                            offset <
                            buffer.byteLength
                        ) {

                            const len =
                                buffer[offset++];


                            if (
                                offset + len <=
                                buffer.byteLength
                            ) {

                                address =
                                    new TextDecoder()
                                        .decode(
                                            buffer.subarray(
                                                offset,
                                                offset + len
                                            )
                                        );


                                offset += len;

                                valid = true;
                            }


                        /*
                         * IPv6
                         */
                        } else if (
                            addrType === 3 &&
                            offset + 15 <
                            buffer.byteLength
                        ) {

                            const parts = [];


                            for (
                                let i = 0;
                                i < 8;
                                i++
                            ) {

                                parts.push(
                                    (
                                        (
                                            buffer[
                                                offset +
                                                i * 2
                                            ] << 8
                                        ) |
                                        buffer[
                                            offset +
                                            i * 2 +
                                            1
                                        ]
                                    ).toString(16)
                                );
                            }


                            address =
                                parts.join(":");


                            offset += 16;

                            valid = true;
                        }


                        if (valid) {

                            const rawData =
                                buffer.subarray(
                                    offset
                                );


                            parsed = {
                                command,
                                port,
                                address,
                                rawData
                            };


                            break;
                        }
                    }
                }
            }
        }


        if (done) {
            break;
        }
    }


    if (!parsed) {

        return new Response(
            "Bad Request",
            {
                status: 400
            }
        );
    }


    const {
        command,
        port,
        address,
        rawData
    } = parsed;


    /*
     * UDP 不支持
     */
    if (command === 2) {

        return new Response(
            "UDP Not Supported",
            {
                status: 403
            }
        );
    }


    let socket;


    /*
     * 首先连接目标地址
     */
    try {

        socket =
            connect({
                hostname: address,
                port: port
            });


        await socket.opened;


    } catch (err) {

        /*
         * 目标连接失败，
         * 使用 KV 中的 p
         */
        if (proxyIP) {

            try {

                socket =
                    connect({
                        hostname: proxyIP,
                        port: proxyPort
                    });


                await socket.opened;


            } catch (fErr) {

                return new Response(
                    "Proxy IP Failed",
                    {
                        status: 502
                    }
                );
            }

        } else {

            return new Response(
                "Connect Failed",
                {
                    status: 502
                }
            );
        }
    }


    try {

        /*
         * 发送已经读取的数据
         */
        const writer =
            socket.writable.getWriter();


        if (
            rawData.byteLength > 0
        ) {

            await writer.write(
                rawData
            );
        }


        writer.releaseLock();


        /*
         * 继续接收客户端数据
         */
        const restOfRequest =
            new ReadableStream({

                async pull(controller) {

                    try {

                        const {
                            value,
                            done
                        } = await reader.read();


                        if (done) {

                            controller.close();

                        } else {

                            controller.enqueue(
                                value
                            );
                        }

                    } catch (e) {

                        controller.error(e);
                    }
                },


                cancel() {

                    reader.cancel();
                }
            });


        restOfRequest
            .pipeTo(
                socket.writable
            )
            .catch(() => {});


        /*
         * TCP -> gRPC
         */
        const responseStream =
            socket.readable.pipeThrough(
                createGrpcWrapper()
            );


        return new Response(
            responseStream,
            {
                status: 200,

                headers: {
                    "Content-Type":
                        "application/grpc"
                }
            }
        );


    } catch (e) {

        return new Response(
            "Stream Error",
            {
                status: 502
            }
        );
    }
}


function createGrpcUnwrapper() {

    let leftover = null;


    return new TransformStream({

        transform(
            chunk,
            controller
        ) {

            let buffer;


            if (leftover) {

                const t =
                    new Uint8Array(
                        leftover.byteLength +
                        chunk.byteLength
                    );


                t.set(leftover);


                t.set(
                    chunk,
                    leftover.byteLength
                );


                buffer = t;

            } else {

                buffer = chunk;
            }


            let offset = 0;


            while (
                offset + 5 <=
                buffer.byteLength
            ) {

                const length =
                    (
                        (
                            buffer[
                                offset + 1
                            ] << 24
                        ) |
                        (
                            buffer[
                                offset + 2
                            ] << 16
                        ) |
                        (
                            buffer[
                                offset + 3
                            ] << 8
                        ) |
                        buffer[
                            offset + 4
                        ]
                    ) >>> 0;


                if (
                    offset +
                    5 +
                    length <=
                    buffer.byteLength
                ) {

                    const frame =
                        buffer.subarray(
                            offset + 5,
                            offset + 5 + length
                        );


                    if (
                        frame.byteLength > 0 &&
                        frame[0] === 0x0A
                    ) {

                        let p = 1;


                        while (
                            p <
                                frame.byteLength &&
                            (
                                frame[p] &
                                0x80
                            ) !== 0
                        ) {

                            p++;
                        }


                        p++;


                        if (
                            p <
                            frame.byteLength
                        ) {

                            controller.enqueue(
                                frame.subarray(p)
                            );
                        }
                    }


                    offset +=
                        5 + length;

                } else {

                    break;
                }
            }


            leftover =
                offset <
                buffer.byteLength
                    ? buffer.subarray(offset)
                    : null;
        }
    });
}


function createGrpcWrapper() {

    return new TransformStream({

        start(controller) {

            controller.enqueue(
                createGrpcFrame(
                    new Uint8Array([0, 0])
                )
            );
        },


        transform(
            chunk,
            controller
        ) {

            controller.enqueue(
                createGrpcFrame(chunk)
            );
        }
    });
}


function createGrpcFrame(rawData) {

    const L =
        rawData.byteLength;


    let varintLen = 1;

    let n = L;


    while (n >= 0x80) {

        varintLen++;

        n >>>= 7;
    }


    const M =
        1 +
        varintLen +
        L;


    const frame =
        new Uint8Array(
            5 + M
        );


    frame[0] = 0;


    frame[1] =
        (M >>> 24) & 0xff;


    frame[2] =
        (M >>> 16) & 0xff;


    frame[3] =
        (M >>> 8) & 0xff;


    frame[4] =
        M & 0xff;


    frame[5] = 0x0A;


    let offset = 6;

    n = L;


    while (n >= 0x80) {

        frame[offset++] =
            (n & 0x7f) | 0x80;

        n >>>= 7;
    }


    frame[offset++] = n;


    frame.set(
        rawData,
        offset
    );


    return frame;
}
