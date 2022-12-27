// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.5.0 <0.9.0;

contract Event {
    event Log(address indexed sender, string message);
    event CustomEvent();

    function test() public {
        emit Log(msg.sender, "Hello World!");
        emit CustomEvent();
    }
}
