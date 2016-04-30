import "unittest" =~ [=> unittest]
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

def makeTokenBucket(maximumSize :(Int > 0), refillRate :Double) as DeepFrozen:
    "Produce a token bucket.

     The bucket regenerates `refillRate` tokens/second while running, up to
     `maximumSize` tokens total."
    def secondsPerToken :Double := 1 / refillRate

    var currentSize :Int := maximumSize
    var resolvers := []
    var loopingCall := null
    var timer := null
    var leftovers := 0.0

    def considerScheduling
    def replenish(count :(Int > 0)) :Void:
        "The ability to refill the token bucket."

        # Do the actual fill.
        if (currentSize < maximumSize):
            currentSize += count
            # Restore the bucket invariant.
            if (currentSize >= maximumSize):
                currentSize := maximumSize

        var newResolvers := []
        for i => [r, count] in (resolvers):
            if (currentSize >= count):
                traceln(`Crediting $count from ($currentSize/$maximumSize)`)
                currentSize -= count
                r.resolve(null)
            else:
                newResolvers := resolvers.slice(i, resolvers.size())
                break
        resolvers := newResolvers
        considerScheduling()

    bind considerScheduling() :Void:
        if (timer != null && currentSize != maximumSize):
            when (def p := timer.fromNow(secondsPerToken)) ->
                def elapsed :(Double > 0.0) := p
                var count :Int := elapsed // secondsPerToken
                # Put a modicum of effort into compensating.
                leftovers += elapsed - (count * secondsPerToken)
                if (leftovers > secondsPerToken):
                    leftovers -= secondsPerToken
                    count += 1
                traceln(`Replenishing $count after $elapsed ($leftovers left over)`)
                replenish(count)

    return object tokenBucket:
        to _printOn(out) :Void:
            def s := `<token bucket $currentSize/$maximumSize ($refillRate t/s)>`
            out.print(s)

        to maximumSize() :(Int > 0):
            return maximumSize

        to backlog() :(Int >= 0):
            "The number of tokens already spoken for via `.willDeduct/1`."
            var i :Int := 0
            for [_, count] in resolvers:
                i += count
            return i

        to deduct(count :(1..maximumSize)) :Bool:
            traceln(`deduct($count): $currentSize/$maximumSize`)
            if (count <= currentSize):
                currentSize -= count
                considerScheduling()
                return true
            return false

        to start(t) :Void:
            timer := t
            considerScheduling()

        to stop() :Void:
            timer := null

        to willDeduct(count :(1..maximumSize)) :Any:
            def [p, r] := Ref.promise()
            resolvers with= ([r, count])
            return p


var clockPromises := [].diverge()
object clock:
    to fromNow(duration :Double):
        def [p, r] := Ref.promise()
        # [resolver, time remaining, total time sitting in queue]
        clockPromises.push([r, duration, 0.0])
        return p

    to advance(amount :Double):
        def unready := [].diverge()
        def ready := [].diverge()
        for [r, var remaining, var spent] in (clockPromises):
            remaining -= amount
            spent += amount
            if (remaining <= 0.0):
                ready.push([r, spent])
            else:
                unready.push([r, remaining, spent])
        clockPromises := unready
        return promiseAllFulfilled([for [r, spent] in (ready)
                                    r<-resolve(spent)])

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
    return when (clock.advance(1.0), p) ->
        null

def testTokenBucketWillDeductDry(assert):
    # Three tokens max, one per second.
    def tb := makeTokenBucket(3, 1.0)
    tb.start(clock)
    # Deduct three, to empty the bucket.
    assert.equal(tb.deduct(3), true)
    # Let's do a couple of .willDeduct() while the bucket is empty.
    clock<-advance(1.0)
    return when (tb.willDeduct(1)) ->
        clock<-advance(2.0)
        when (tb.willDeduct(2)) ->
            clock<-advance(3.0)
            tb.willDeduct(3)

def testTokenBucketWillDeductBoneDry(assert):
    # Three tokens max, one per second.
    def tb := makeTokenBucket(3, 1.0)
    tb.start(clock)
    # Deduct three, to empty the bucket.
    assert.equal(tb.deduct(3), true)
    # Let's do some .willDeduct() while the bucket is empty. We force
    # callbacks to be delayed by a turn by also delaying the clock
    # advancement.
    def x := tb.willDeduct(3)
    def y := tb.willDeduct(3)
    def z := tb.willDeduct(3)
    # Refill the bucket thrice.
    clock<-advance(3.0)
    return when (x) ->
        clock<-advance(3.0)
        when (y) ->
            clock<-advance(3.0)
            when (z) ->
                null

unittest([
    testTokenBucket,
    testTokenBucketWillDeduct,
    testTokenBucketWillDeductDry,
    testTokenBucketWillDeductBoneDry,
])
