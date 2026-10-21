# P2P Network (从 exo 解耦的分布式网络与文件传输模块)

本项目是将 [exo](https://github.com/exo-explore/exo) 项目中的底层 Rust 网络栈（`exo/rust` 下的 UDP Multicast 自动组网 + Zenoh P2P 拓扑通信）完全解耦出来的独立工程。

它提供：
1. **局域网多设备零配置自发现**：通过 IPv6 链路本地组播（Multicast），无需集中式注册中心即可自动建立 P2P 全互联拓扑。
2. **设备动态加入与退出检测**：基于 Zenoh Liveliness Token 机制，实时感知新节点加入与异常/正常断开（输出节点 ID）。
3. **二进制文件分块可靠传输**：原生支持 `.deb`、`.tar`、`.tar.gz` 等大文件与安装包分块传输，并带有 **SHA-256 哈希校验** 与写入重命名保护。
4. **Python 原生绑定调用**：基于 PyO3 将底层高性能异步网络封装，在 Python 侧以极简的 API 进行调用。

---

## 目录结构说明

将整个 `my_p2p_network` 文件夹复制到干净机器即可，目录各部分说明如下：

```text
my_p2p_network/
├── Cargo.toml               # Rust Workspace 根配置文件（包含依赖与 git 补丁）
├── Cargo.lock               # 依赖版本锁定文件（确保构建版本一致）
├── build.sh                 # 【一键编译脚本】自动编译 Rust 并输出 Python 扩展包
├── run_node.py              # 【交互式命令行测试程序】包含节点发现、发文件、聊天等功能
├── test_p2p.py              # 自动化测试脚本
├── rust/
│   ├── networking/          # 纯 Rust 核心网络层（UDP 发现、Zenoh 会话、存活检测）
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── discovery.rs # UDP 组播收发与接口监听
│   │       ├── swarm.rs     # Zenoh 拓扑、主题订阅与存活(Liveliness)事件
│   │       └── lib.rs       # 模块入口与网络配置
│   └── p2p_rs/              # PyO3 Python 原生扩展绑定层
│       ├── Cargo.toml
│       └── src/
│           ├── networking.rs      # PyNetworkingHandle 与 PyFromSwarm 封装
│           └── lib.rs             # Python 模块导出定义
└── python/
    └── p2p_network/         # 高级 Python 模块
        ├── __init__.py      # 包入口
        ├── node.py          # P2PNode 节点管理核心类
        ├── file_transfer.py # 分块文件传输管理器（SHA256 校验）
        └── p2p_rs.so        # 编译生成的底层动态链接库（运行 build.sh 后生成）
```

---

## 一、 干净 Linux 系统的环境准备

在将本代码拷贝到一台全新的 Linux 机器前，请先安装基础依赖环境：

### 1. 安装系统基础编译工具
以 **Ubuntu / Debian** 为例：
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config libssl-dev clang git python3 python3-pip
```

以 **CentOS / RHEL / Fedora** 为例：
```bash
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y cmake pkgconfig openssl-devel clang git python3 python3-pip
```

### 2. 安装 Rust 工具链（推荐 1.80+）
如果系统尚未安装 Rust，执行官方安装脚本：
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# 验证安装
rustc --version
cargo --version
```

### 3. 局域网网络与防火墙要求（极其重要！）
1. **Wi-Fi / 局域网路由器要求**：
   * 多台机器必须连接在**同一个局域网（同一路由器/交换机）**。
   * **路由器不能开启 AP 隔离（AP Isolation / Client Isolation）**。
   * 局域网需支持 **IPv6 链路本地组播（Multicast）**（一般的家用/办公路由器默认开启）。
2. **防火墙端口放行**：
   * **UDP 52413**：组播发现端口。
   * **TCP 52414**：节点间数据通道端口。
   
   在 Ubuntu 上开放端口（如果启用了 `ufw`）：
   ```bash
   sudo ufw allow 52413/udp
   sudo ufw allow 52414/tcp
   ```

---

## 二、 编译流程

在 `my_p2p_network/` 目录下，直接执行我们提供的构建脚本：

```bash
cd my_p2p_network
chmod +x build.sh
./build.sh
```

### 构建脚本做了什么？
1. 执行 `cargo build --release`，编译生成高性能的 `target/release/libp2p_rs.so`。
2. 自动将其复制并重命名为 `python/p2p_network/p2p_rs.so`，供 Python 直接导入。

*(若手动编译，步骤等价于：`cargo build --release && cp target/release/libp2p_rs.so python/p2p_network/p2p_rs.so`)*

---

## 三、 运行与多设备联调

### 1. 运行交互式节点
在**机器 A** 上打开终端运行：
```bash
python3 run_node.py
```
控制台将输出：
```text
============================================================
🌐 P2P Node Online: a1b2c3d4e5f67890
   Namespace:       my_p2p_network
   Listen Port:     TCP 52414
   Discovery Port:  UDP 52413
   Download Folder: /home/user/my_p2p_network/downloads
============================================================

Interactive Commands:
  nodes                   - Show active nodes in cluster
  send <file_path>        - Send a file (.deb, .tar, etc.) to the cluster
  msg <text>              - Broadcast a message on 'chat' topic
  quit / exit             - Exit node

p2p> 
```

### 2. 动态发现节点加入
在**同一局域网的机器 B** 上同样运行：
```bash
python3 run_node.py
```
此时，**机器 A** 的终端会立即捕获到底层 Zenoh Liveliness 事件并打印：
```text
🔔 [CLUSTER UPDATE] New device connected: f9e8d7c6b5a43210
📊 Current active devices (1): ['f9e8d7c6b5a43210']
```
在任意一台机器的 `p2p>` 提示符下输入：
```text
p2p> nodes
```
即可查看当前局域网内所有在线节点 ID。

### 3. 传输 `.deb`、`.tar` 等文件（支持群发所有设备 / 定向发送单台设备）
准备一个 `.deb` 或 `.tar` 软件包（例如 `package.deb` 或 `archive.tar`）。

#### 方式 A：群发给集群内的所有设备（默认广播）
在发送方终端输入：
```text
p2p> send /path/to/my_package.deb
```
此时网络中的**所有在线机器（机器 B、机器 C、机器 D...）都会同时并行接收该文件**，并各自独立校验 SHA-256 保存到各自的 `./downloads/` 目录！

#### 方式 B：定向发送给特定某一台设备
如果只想发给某一台设备，先用 `nodes` 查看其 ID，然后指定目标 ID：
```text
p2p> send /path/to/my_package.deb f9e8d7c6b5a43210
```
此时仅该目标设备会接收并保存文件，其他设备会自动忽略该传输。

* **发送端**会计算 SHA-256 并分块上传：
  ```text
  Computing SHA-256 for 'my_package.deb' (12.50 MB)...
  🚀 [Sending File] 'my_package.deb' (12.50 MB) in 100 chunks
  📤 [Uploading] 'my_package.deb': 20/100 chunks (20.0%)
  ...
  ✨ [File Sent Finished] 'my_package.deb'
  ```
* **接收端**会自动接收分块并校验哈希保存：
  ```text
  📥 [Incoming File] 'my_package.deb' (12.50 MB) from node 'a1b2c3d4e5f67890'
  ⏳ [Downloading] 'my_package.deb': 40/100 chunks (40.0%)
  ...
  ✅ [File Received Successfully] 'my_package.deb' (12.50 MB, 48.21 MB/s)
  🎉 [File Saved] 'my_package.deb' (13107200 bytes)
     Saved to: /home/user/my_p2p_network/downloads/my_package.deb
     SHA256:   e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
  ```

### 4. 动态检测设备退出/剔除
在**机器 B** 上按下 `Ctrl+C` 或输入 `quit` 退出程序（或者直接断开机器 B 的网络）。

**机器 A** 会在 1~3 秒内感知到底层心跳断开，自动将该节点从活跃节点列表中剔除：
```text
⚠️  [CLUSTER UPDATE] Device disconnected: f9e8d7c6b5a43210
📊 Remaining active devices (0): []
```

---

## 四、 在自己的 Python 代码中使用 P2PNode

你可以直接将 `python/p2p_network` 作为模块导入到你的业务系统：

```python
import asyncio
from p2p_network import P2PNode

async def main():
    # 1. 创建节点（端口与目录均可自定义）
    node = P2PNode(
        namespace="my_cluster",
        listen_port=52414,
        discovery_port=52413,
        download_dir="./downloads"
    )

    # 2. 注册设备上线/离线回调
    node.on_peer_join = lambda peer_id: print(f"🟢 设备上线: {peer_id}")
    node.on_peer_leave = lambda peer_id: print(f"🔴 设备下线: {peer_id}")

    # 3. 注册文件接收成功回调
    def on_file(info):
        print(f"📦 收到文件: {info['filename']}, 大小: {info['file_size']} 字节")
        print(f"   保存路径: {info['saved_to']}")
        print(f"   SHA256:   {info['sha256']}")
    node.on_file_received = on_file

    # 4. 启动节点
    await node.start()

    # 5. 发送文件（例如向集群广播发送 deb 安装包）
    # await node.send_file("/tmp/nginx_1.18.deb")

    # 持续运行监听
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 五、 常见排查与 FAQ

1. **两台机器无法互相发现？**
   * **排查 1（Wi-Fi 隔离）**：很多公共 Wi-Fi 或公司访客 Wi-Fi 会拦截客户端间的相互通讯。请使用手机开启热点让两台电脑连接，或者连接同一台家用路由器测试。
   * **排查 2（防火墙）**：临时关闭防火墙测试（`sudo ufw disable`），若能发现则说明是端口放行问题。
   * **排查 3（IPv6 组播）**：确保网络接口开启了 IPv6，运行 `ip -6 addr` 能看到形如 `fe80::...` 的链路本地地址。

2. **如何修改组网的局域网组播地址？**
   * 在 [rust/networking/src/discovery.rs](rust/networking/src/discovery.rs) 中搜索 `GROUP` 常量，默认为 `0xff12::e0a1:de89`。若需隔离不同的 P2P 网络，只需修改 `namespace` 参数，不同 `namespace` 的节点互不干扰。
