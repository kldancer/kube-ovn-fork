package util

import (
	"fmt"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	"net"
	"syscall"

	"github.com/florianl/go-tc"
	"github.com/florianl/go-tc/core"
)

func closeTCClient(tcc *tc.Tc) {
	if err := tcc.Close(); err != nil {
		klog.Errorf("Could not close tc client: %v\n", err)
	}
}

func EnsureNodeIfTc(ifName string, maxRate int) error {
	tcc, err := tc.Open(&tc.Config{})
	if err != nil {
		return err
	}
	defer closeTCClient(tcc)

	link, err := net.InterfaceByName(ifName)
	if err != nil {
		return err
	}

	err = tcc.Qdisc().Replace(&tc.Object{
		Msg: tc.Msg{
			Family:  unix.AF_UNSPEC,
			Ifindex: uint32(link.Index),
			Handle:  core.BuildHandle(0x1, 0x0),
			Parent:  tc.HandleRoot,
			Info:    0,
		},
		Attribute: tc.Attribute{
			Kind: "htb",
			Htb: &tc.Htb{
				Init: &tc.HtbGlob{
					Version:      0x3,
					Rate2Quantum: 0xa,
					Defcls:       0x10, // <--- 就是这里！指定 default=0x10
				},
			},
		},
	})
	if err != nil {
		return err
	}

	classIDs := []uint32{0xfffe, 0x10}
	// 单位 bit/sec, 9000mbit
	toByte := uint32(1000 * 1000 / 8)
	for i, cid := range classIDs {
		parent := uint32(0x1)
		if i > 0 {
			parent = core.BuildHandle(0x1, 0xfffe)
		}

		err = tcc.Class().Replace(&tc.Object{
			Msg: tc.Msg{
				Family:  unix.AF_UNSPEC,
				Ifindex: uint32(link.Index),
				Parent:  parent,
				Handle:  core.BuildHandle(0x1, cid),
				Info:    0,
			},
			Attribute: tc.Attribute{
				Kind: "htb",
				Htb: &tc.Htb{
					Parms: &tc.HtbOpt{
						Rate: tc.RateSpec{
							Rate: uint32(maxRate) * toByte,
						},
						Ceil: tc.RateSpec{
							Rate: uint32(maxRate) * toByte,
						},
					},
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteNodeIfTc(ifName string) error {
	tcc, err := tc.Open(&tc.Config{})
	if err != nil {
		return err
	}
	defer closeTCClient(tcc)

	link, err := net.InterfaceByName(ifName)
	if err != nil {
		return err
	}

	return tcc.Qdisc().Delete(&tc.Object{
		Msg: tc.Msg{
			Family:  unix.AF_UNSPEC,
			Ifindex: uint32(link.Index),
			Handle:  core.BuildHandle(0x1, 0x0),
			Parent:  tc.HandleRoot,
			Info:    0,
		},
		Attribute: tc.Attribute{
			Kind: "htb",
			Htb: &tc.Htb{
				Init: &tc.HtbGlob{
					Version:      0x3,
					Rate2Quantum: 0xa,
				},
			},
		},
	})
}

func AddIfTcFilter(ifName string, mark uint32) error {
	tcc, err := tc.Open(&tc.Config{})
	if err != nil {
		return err
	}
	defer closeTCClient(tcc)

	iface, err := net.InterfaceByName(ifName)
	if err != nil {
		return fmt.Errorf("faile to get interface: %v", err)
	}
	linkIdx := uint32(iface.Index)

	// tc filter add dev eno1 parent 1: protocol ip prio 1 handle 10 fw flowid 1:2
	classId := core.BuildHandle(1, 2)

	return tcc.Filter().Add(&tc.Object{
		Msg: tc.Msg{
			Family:  syscall.AF_UNSPEC,
			Ifindex: linkIdx,
			// parent qdisc 是 1:
			Parent: core.BuildHandle(1, 0),
			// Msg.Handle 用来传递 fw‐mark（这里就是 10）
			Handle: core.BuildHandle(0, mark),
			// Msg.Info 高 16 位是 prio，低 16 位是协议号 ETH_P_IP
			Info: uint32(1<<16) | uint32(htons(uint16(syscall.ETH_P_IP))),
		},
		Attribute: tc.Attribute{
			Kind: "fw",
			Fw: &tc.Fw{
				// 匹配后要走到的 class，也就是 flowid 1:2
				ClassID: &classId,
			},
		},
	})
}

func htons(val uint16) uint16 {
	return (val<<8)&0xff00 | val>>8
}
