package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/skikozou/enc52"
)

type Client struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
}

type Token struct {
	IP        string
	Port      uint16
	Timestamp int64
}

func NewClient(addr string) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %w", err)
	}

	udpConn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial UDP: %w", err)
	}

	return &Client{
		conn:       udpConn,
		remoteAddr: udpAddr,
	}, nil
}

func NewClientListen(addr string) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address: %w", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen UDP: %w", err)
	}

	return &Client{
		conn:       udpConn,
		remoteAddr: nil,
	}, nil
}

func (c *Client) Write(data []byte) (int, error) {
	if c.remoteAddr != nil {
		return c.conn.Write(data)
	}
	return 0, fmt.Errorf("no remote address set")
}

func (c *Client) WriteString(text string) (int, error) {
	return c.Write([]byte(text))
}

func (c *Client) WriteTo(data []byte, addr *net.UDPAddr) (int, error) {
	return c.conn.WriteToUDP(data, addr)
}

func (c *Client) WriteStringTo(text string, addr *net.UDPAddr) (int, error) {
	return c.WriteTo([]byte(text), addr)
}

func (c *Client) Read(buf []byte) (int, *net.UDPAddr, error) {
	n, addr, err := c.conn.ReadFromUDP(buf)
	return n, addr, err
}

func (c *Client) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *Client) SetRemoteAddr(addr *net.UDPAddr) {
	c.remoteAddr = addr
}

func (c *Client) LocalAddr() *net.UDPAddr {
	return c.conn.LocalAddr().(*net.UDPAddr)
}

func (c *Client) RemoteAddr() *net.UDPAddr {
	return c.remoteAddr
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func GenerateToken(addr *net.UDPAddr) *Token {
	return &Token{
		IP:        addr.IP.String(),
		Port:      uint16(addr.Port),
		Timestamp: time.Now().Unix(),
	}
}

func (t *Token) ToAddr() *net.UDPAddr {
	return &net.UDPAddr{
		IP:   net.ParseIP(t.IP),
		Port: int(t.Port),
	}
}

func (t *Token) IsExpired(maxAge time.Duration) bool {
	age := time.Since(time.Unix(t.Timestamp, 0))
	return age > maxAge
}

func getOutboundIP() (net.IP, error) {
	// ダミー接続を作って自分の外部向けIPを取得
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, nil
}

func encodeToken(token *Token) string {
	raw := fmt.Sprintf("%s?%d?%d", token.IP, token.Port, token.Timestamp)
	logrus.WithField("raw", raw).Debug("encoding token")
	encoded := enc52.Encode(raw)
	logrus.WithField("encoded", encoded).Debug("token encoded")
	return encoded
}

func decodeToken(encoded string) (*Token, error) {
	logrus.WithField("encoded", encoded).Debug("decoding token")

	decoded, err := enc52.Decode(encoded)
	if err != nil {
		logrus.WithError(err).Error("enc52 decode failed")
		return nil, fmt.Errorf("enc52 decode failed: %w", err)
	}

	logrus.WithField("decoded", decoded).Info("enc52 decode success")

	parts := strings.Split(decoded, "?")
	if len(parts) != 3 {
		logrus.WithField("parts", len(parts)).Error("invalid token format")
		return nil, fmt.Errorf("invalid token format: expected 3 parts, got %d", len(parts))
	}

	port, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		logrus.WithError(err).WithField("port_str", parts[1]).Error("invalid port")
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	timestamp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		logrus.WithError(err).WithField("timestamp_str", parts[2]).Error("invalid timestamp")
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	token := &Token{
		IP:        parts[0],
		Port:      uint16(port),
		Timestamp: timestamp,
	}

	logrus.WithFields(logrus.Fields{
		"ip":        token.IP,
		"port":      token.Port,
		"timestamp": token.Timestamp,
	}).Debug("token parsed successfully")

	return token, nil
}

func holePunch(client *Client, peerAddr *net.UDPAddr) error {
	logrus.Info("Starting hole punching...")

	// 複数回パケットを送信して穴を開ける
	for i := 0; i < 100; i++ {
		_, err := client.WriteStringTo("PUNCH", peerAddr)
		if err != nil {
			logrus.WithError(err).Warn("hole punch failed")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 受信待機（タイムアウト付き）
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)

	for {
		n, addr, err := client.Read(buf)
		if err != nil {
			return fmt.Errorf("failed to receive response: %w", err)
		}

		logrus.WithField("from", addr).WithField("data", string(buf[:n])).Info("received packet")

		if string(buf[:n]) == "PUNCH" {
			logrus.Info("Hole punching successful!")
			client.SetRemoteAddr(addr)
			return nil
		}
	}
}

func communicationLoop(client *Client) {
	// 受信ゴルーチン
	go func() {
		buf := make([]byte, 4096)
		for {
			n, addr, err := client.Read(buf)
			if err != nil {
				logrus.WithError(err).Error("failed to read")
				return
			}

			fmt.Printf("\n[%s] %s", addr, string(buf[:n]))
		}
	}()

	// 送信ループ
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter message (Ctrl+C to exit):")

	for scanner.Scan() {
		message := scanner.Text()
		_, err := client.WriteString(message + "\n")
		if err != nil {
			logrus.WithError(err).Error("failed to send")
			continue
		}
	}
}

func logrusInit() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors:            true,
		DisableLevelTruncation: true,
		PadLevelText:           true,
		FullTimestamp:          true,
	})
}

func main() {
	logrusInit()
	logrus.Info("UDP Hole Punching Client")

	// 1. 自分の外部向けIPを取得
	myIP, err := getOutboundIP()
	if err != nil {
		logrus.WithError(err).Fatal("failed to get outbound IP")
	}
	logrus.WithField("ip", myIP).Info("detected outbound IP")

	// 2. UDPソケット作成（リスニング）
	client, err := NewClientListen(":0") // ランダムポート
	if err != nil {
		logrus.WithError(err).Fatal("failed to create client")
	}
	defer client.Close()

	localAddr := client.LocalAddr()
	logrus.WithField("address", localAddr).Info("listening on")

	// 3. 自分のトークン生成（IPを実際のものに置き換え）
	myToken := &Token{
		IP:        myIP.String(),
		Port:      uint16(localAddr.Port),
		Timestamp: time.Now().Unix(),
	}
	logrus.WithFields(logrus.Fields{
		"ip":        myToken.IP,
		"port":      myToken.Port,
		"timestamp": myToken.Timestamp,
	}).Debug("generated token")

	encodedToken := encodeToken(myToken)

	fmt.Println("========================================")
	fmt.Printf("Your token: %s\n", encodedToken)
	fmt.Println("========================================")

	// 4. 相手のトークン入力待ち
	fmt.Print("\nEnter peer token: ")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		logrus.Fatal("failed to read input")
	}
	peerTokenStr := strings.TrimSpace(scanner.Text())
	logrus.WithField("input", peerTokenStr).Debug("received peer token input")

	// 5. 相手トークンデコード
	peerToken, err := decodeToken(peerTokenStr)
	if err != nil {
		logrus.WithError(err).Fatal("failed to decode peer token")
	}

	// タイムスタンプ検証（10分以内）
	if peerToken.IsExpired(10 * time.Minute) {
		logrus.Fatal("peer token is expired")
	}

	peerAddr := peerToken.ToAddr()
	logrus.WithField("peer", peerAddr).Info("peer address resolved")

	// 6. ホールパンチング開始
	err = holePunch(client, peerAddr)
	if err != nil {
		logrus.WithError(err).Fatal("hole punching failed")
	}

	// 7. 接続確立
	logrus.Info("Connection established!")

	// タイムアウト解除
	client.SetReadDeadline(time.Time{})

	// 8. データ送受信ループ
	communicationLoop(client)
}
