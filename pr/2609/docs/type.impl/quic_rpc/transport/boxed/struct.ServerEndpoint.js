(function() {var type_impls = {
"iroh":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Clone-for-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-Clone-for-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a> for ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: RpcMessage,\n    Out: RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone\" class=\"method trait-impl\"><a href=\"#method.clone\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\" class=\"fn\">clone</a>(&amp;self) -&gt; ServerEndpoint&lt;In, Out&gt;</h4></section></summary><div class='docblock'>Returns a copy of the value. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone_from\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/nightly/src/core/clone.rs.html#169\">source</a></span><a href=\"#method.clone_from\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\" class=\"fn\">clone_from</a>(&amp;mut self, source: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.reference.html\">&amp;Self</a>)</h4></section></summary><div class='docblock'>Performs copy-assignment from <code>source</code>. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\">Read more</a></div></details></div></details>","Clone","iroh::node::IrohServerEndpoint"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-ConnectionCommon%3CIn,+Out%3E-for-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-ConnectionCommon%3CIn,+Out%3E-for-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; ConnectionCommon&lt;In, Out&gt; for ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: RpcMessage,\n    Out: RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle\" open><summary><section id=\"associatedtype.RecvStream\" class=\"associatedtype trait-impl\"><a href=\"#associatedtype.RecvStream\" class=\"anchor\">§</a><h4 class=\"code-header\">type <a class=\"associatedtype\">RecvStream</a> = RecvStream&lt;In&gt;</h4></section></summary><div class='docblock'>Receive side of a bidirectional typed channel</div></details><details class=\"toggle\" open><summary><section id=\"associatedtype.SendSink\" class=\"associatedtype trait-impl\"><a href=\"#associatedtype.SendSink\" class=\"anchor\">§</a><h4 class=\"code-header\">type <a class=\"associatedtype\">SendSink</a> = SendSink&lt;Out&gt;</h4></section></summary><div class='docblock'>Send side of a bidirectional typed channel</div></details></div></details>","ConnectionCommon<In, Out>","iroh::node::IrohServerEndpoint"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-ConnectionErrors-for-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-ConnectionErrors-for-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; ConnectionErrors for ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: RpcMessage,\n    Out: RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle\" open><summary><section id=\"associatedtype.OpenError\" class=\"associatedtype trait-impl\"><a href=\"#associatedtype.OpenError\" class=\"anchor\">§</a><h4 class=\"code-header\">type <a class=\"associatedtype\">OpenError</a> = <a class=\"struct\" href=\"https://docs.rs/anyhow/1.0.85/anyhow/struct.Error.html\" title=\"struct anyhow::Error\">Error</a></h4></section></summary><div class='docblock'>Error when opening or accepting a channel</div></details><details class=\"toggle\" open><summary><section id=\"associatedtype.SendError\" class=\"associatedtype trait-impl\"><a href=\"#associatedtype.SendError\" class=\"anchor\">§</a><h4 class=\"code-header\">type <a class=\"associatedtype\">SendError</a> = <a class=\"struct\" href=\"https://docs.rs/anyhow/1.0.85/anyhow/struct.Error.html\" title=\"struct anyhow::Error\">Error</a></h4></section></summary><div class='docblock'>Error when sending a message via a channel</div></details><details class=\"toggle\" open><summary><section id=\"associatedtype.RecvError\" class=\"associatedtype trait-impl\"><a href=\"#associatedtype.RecvError\" class=\"anchor\">§</a><h4 class=\"code-header\">type <a class=\"associatedtype\">RecvError</a> = <a class=\"struct\" href=\"https://docs.rs/anyhow/1.0.85/anyhow/struct.Error.html\" title=\"struct anyhow::Error\">Error</a></h4></section></summary><div class='docblock'>Error when receiving a message via a channel</div></details></div></details>","ConnectionErrors","iroh::node::IrohServerEndpoint"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Debug-for-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-Debug-for-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> for ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> + RpcMessage,\n    Out: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> + RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>, <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/core/fmt/struct.Error.html\" title=\"struct core::fmt::Error\">Error</a>&gt;</h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\">Read more</a></div></details></div></details>","Debug","iroh::node::IrohServerEndpoint"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-ServerEndpoint%3CIn,+Out%3E-for-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-ServerEndpoint%3CIn,+Out%3E-for-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; ServerEndpoint&lt;In, Out&gt; for ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: RpcMessage,\n    Out: RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.accept\" class=\"method trait-impl\"><a href=\"#method.accept\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a class=\"fn\">accept</a>(\n    &amp;self\n) -&gt; impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/future/future/trait.Future.html\" title=\"trait core::future::future::Future\">Future</a>&lt;Output = <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/result/enum.Result.html\" title=\"enum core::result::Result\">Result</a>&lt;(&lt;ServerEndpoint&lt;In, Out&gt; as ConnectionCommon&lt;In, Out&gt;&gt;::SendSink, &lt;ServerEndpoint&lt;In, Out&gt; as ConnectionCommon&lt;In, Out&gt;&gt;::RecvStream), &lt;ServerEndpoint&lt;In, Out&gt; as ConnectionErrors&gt;::OpenError&gt;&gt; + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a></h4></section></summary><div class='docblock'>Accept a new typed bidirectional channel on any of the connections we\nhave currently opened.</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.local_addr\" class=\"method trait-impl\"><a href=\"#method.local_addr\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a class=\"fn\">local_addr</a>(&amp;self) -&gt; &amp;[LocalAddr]</h4></section></summary><div class='docblock'>The local addresses this endpoint is bound to.</div></details></div></details>","ServerEndpoint<In, Out>","iroh::node::IrohServerEndpoint"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-ServerEndpoint%3CIn,+Out%3E\" class=\"impl\"><a href=\"#impl-ServerEndpoint%3CIn,+Out%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;In, Out&gt; ServerEndpoint&lt;In, Out&gt;<div class=\"where\">where\n    In: RpcMessage,\n    Out: RpcMessage,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.new\" class=\"method\"><h4 class=\"code-header\">pub fn <a class=\"fn\">new</a>(x: impl BoxableServerEndpoint&lt;In, Out&gt;) -&gt; ServerEndpoint&lt;In, Out&gt;</h4></section></summary><div class=\"docblock\"><p>Wrap a boxable server endpoint into a box, transforming all the types to concrete types</p>\n</div></details></div></details>",0,"iroh::node::IrohServerEndpoint"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()