import "unittest" =~ [=> unittest]
import "loopingCall" =~ [=> makeLoopingCall :DeepFrozen]
exports (makeTokenBucket)

# Copyright (C) 2014 Google Inc. All rights reserved.
# Copyright (C) 2015-2016 Corbin Simpson.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

def makeTokenBucket(maximumSize :Int, refillRate :Double) as DeepFrozen:
    "Produce a token bucket.

     The bucket regenerates `refillRate` tokens/second while running, up to
     `maximumSize` tokens total."
    def secondsPerToken :Double := 1 / refillRate

    var currentSize :Int := maximumSize
    var resolvers := []
    var loopingCall := null

    def replenish(count :(Int > 0)) :Void:
        "The ability to refill the token bucket."

        if (currentSize < maximumSize):
            currentSize += count

        for i => [r, count] in (resolvers):
            if (currentSize >= count):
                currentSize -= count
                r.resolve(null)
            else:
                resolvers := resolvers.slice(i, resolvers.size())
                return
        resolvers := []

    return object tokenBucket:
        to getBurstSize() :Int:
            return maximumSize

        to deduct(count :(1..maximumSize)) :Bool:
            # traceln(`deduct($count): $currentSize/$maximumSize`)
            if (count <= currentSize):
                currentSize -= count
                return true
            return false

        to start(timer) :Void:
            loopingCall := makeLoopingCall(timer, fn {replenish(1)})
            loopingCall.start(secondsPerToken)

        to stop() :Void:
            loopingCall.stop()

        to willDeduct(count :(1..maximumSize)) :Any:
            def [p, r] := Ref.promise()
            resolvers with= ([r, count])
            return p


var clockPromises := [].diverge()
object clock:
    to fromNow(duration :Double):
        def [p, r] := Ref.promise()
        clockPromises.push([r, duration])
        return p

    to advance(amount :Double):
        def unready := [].diverge()
        def ready := [].diverge()
        for [r, duration] in (clockPromises):
            def remaining := duration - amount
            if (remaining <= 0.0):
                ready.push(r)
            else:
                unready.push([r, remaining])
        clockPromises := unready
        return promiseAllFulfilled([for r in (ready) r<-resolve(null)])

def testTokenBucket(assert):
    # Three tokens max, one per second.
    def tb := makeTokenBucket(3, 1.0)
    tb.start(clock)
    # Deduct one. Current count should be two.
    assert.equal(tb.deduct(1), true)
    # Deduct two. Current count should be zero.
    assert.equal(tb.deduct(2), true)
    # Deduct one. Should fail.
    assert.equal(tb.deduct(1), false)
    # Refill one token.
    return when (clock.advance(1.0)) ->
        # Deduct one. Current count should be zero.
        assert.equal(tb.deduct(1), true)

def testTokenBucketWillDeduct(assert):
    # Three tokens max, one per second.
    def tb := makeTokenBucket(3, 1.0)
    tb.start(clock)
    # Deduct one. Current count should be two.
    assert.equal(tb.deduct(1), true)
    # Request three.
    def p := tb.willDeduct(3)
    return promiseAllFulfilled([clock.advance(1.0), p])

unittest([
    testTokenBucket,
    testTokenBucketWillDeduct,
])
