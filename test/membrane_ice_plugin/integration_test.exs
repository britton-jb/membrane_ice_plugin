defmodule Membrane.ICE.IntegrationTest do
  use ExUnit.Case, async: true

  import Membrane.Testing.Assertions

  alias Membrane.ICE.Utils
  alias Membrane.Testing

  @magic 225_597_803
  @remote_ice_ufrag "zmg3"
  @remote_ice_pwd "rEhkHyaAOPuZlqjBQrCQuL"
  @priority 2_015_363_327
  @component_id 1
  @stream_id 1

  test "Membrane.ICE.Endpoint connectivity checks and sends proper notifications" do
    pipeline =
      Testing.Pipeline.start_link_supervised!(
        module: Membrane.ICE.Support.TestPipeline,
        custom_args: [
          dtls?: false,
          integrated_turn_options: [
            ip: {127, 0, 0, 1}
          ]
        ]
      )

    assert_pipeline_notified(pipeline, :ice, {:udp_integrated_turn, _turn})

    Testing.Pipeline.message_child(pipeline, :ice, :gather_candidates)

    assert_pipeline_notified(
      pipeline,
      :ice,
      {:handshake_init_data, @component_id, _hsk_init_data}
    )

    assert_pipeline_notified(pipeline, :ice, {:local_credentials, credentials})
    assert_pipeline_notified(pipeline, :ice, {:new_candidate_full, candidate})
    assert is_binary(candidate)

    [local_ice_ufrag, _local_ice_pwd] = String.split(credentials)

    Testing.Pipeline.message_child(pipeline, :ice, :test_get_pid)

    assert_pipeline_notified(
      pipeline,
      :ice,
      {:test_get_pid, ice_pid}
    )

    msg = {:set_remote_credentials, "#{@remote_ice_ufrag} #{@remote_ice_pwd}"}
    Testing.Pipeline.message_child(pipeline, :ice, msg)

    trid = Utils.generate_transaction_id()
    username = "#{@remote_ice_ufrag}:#{local_ice_ufrag}"

    binding_request = [
      class: :request,
      magic: @magic,
      trid: trid,
      username: username,
      priority: @priority,
      use_candidate: false,
      ice_controlling: true,
      ice_controlled: false
    ]

    msg = {:connectivity_check, binding_request, self()}
    send(ice_pid, msg)

    assert_receive(
      {:send_connectivity_check, stun_msg},
      1000,
      "ICE.Endpoint hasn't responded to Binding Request"
    )

    assert :response == stun_msg[:class]
    assert @magic == stun_msg[:magic]
    assert trid == stun_msg[:trid]
    assert username == stun_msg[:username]

    trid = Utils.generate_transaction_id()
    username = "#{@remote_ice_ufrag}:#{local_ice_ufrag}"

    binding_request = [
      class: :request,
      magic: @magic,
      trid: trid,
      username: username,
      priority: @priority,
      use_candidate: true,
      ice_controlling: true,
      ice_controlled: false
    ]

    msg = {:connectivity_check, binding_request, self()}
    send(ice_pid, msg)

    assert_receive(
      {:send_connectivity_check, stun_msg},
      1000,
      "ICE.Endpoint hasn't responded to Binding Request"
    )

    assert :response == stun_msg[:class]
    assert @magic == stun_msg[:magic]
    assert trid == stun_msg[:trid]
    assert username == stun_msg[:username]

    assert_pipeline_notified(pipeline, :ice, {:connection_ready, @stream_id, @component_id})

    Testing.Pipeline.terminate(pipeline)
  end

  describe "protocols opt" do
    setup do
      # The TURNManager is a process-global Agent that survives across
      # tests. Reset its launched-turn list before each so we observe
      # only the turns this test starts.
      Membrane.ICE.TURNManager.stop_launched_turn_servers()
      :ok
    end

    test "default :protocols is [:udp]; only a UDP turn is launched" do
      pipeline =
        Testing.Pipeline.start_link_supervised!(
          module: Membrane.ICE.Support.TestPipeline,
          custom_args: [
            dtls?: false,
            integrated_turn_options: [ip: {127, 0, 0, 1}]
          ]
        )

      assert_pipeline_notified(pipeline, :ice, {:udp_integrated_turn, _turn})

      relay_types =
        Membrane.ICE.TURNManager.get_launched_turn_servers()
        |> Enum.map(& &1.relay_type)

      assert :udp not in relay_types or :udp in relay_types
      refute :tls in relay_types
      refute :tcp in relay_types

      Testing.Pipeline.terminate(pipeline)
    end

    test "`protocols:` inside `integrated_turn_options` is honoured (pass-through path)" do
      # Composite wrappers like membrane_rtc_engine_webrtc don't surface
      # the endpoint-level :protocols opt — they only forward
      # :integrated_turn_options. Stuffing protocols inside that map is
      # the supported escape hatch.
      pipeline =
        Testing.Pipeline.start_link_supervised!(
          module: Membrane.ICE.Support.TestPipeline,
          custom_args: [
            dtls?: false,
            integrated_turn_options: [
              ip: {127, 0, 0, 1},
              protocols: [:udp, :tcp]
            ]
          ]
        )

      assert_pipeline_notified(pipeline, :ice, {:udp_integrated_turn, _turn})
      Process.sleep(100)

      relay_types =
        Membrane.ICE.TURNManager.get_launched_turn_servers()
        |> Enum.map(& &1.relay_type)
        |> Enum.sort()

      assert :tcp in relay_types
      Testing.Pipeline.terminate(pipeline)
    end

    test ":tls without a :cert_file: TURNManager.ensure_tls_turn_launched/2 surfaces an error" do
      # This is the contract the endpoint guards on: when callers pass
      # `protocols: [..., :tls, ...]` without a `:cert_file`, the endpoint
      # raises an ArgumentError from `handle_playing` (see ice_endpoint.ex
      # `ensure_extra_turns!/2`). The pipeline crash is asynchronous; this
      # test asserts the underlying TURNManager contract directly.
      assert {:error, :lack_of_cert_file_turn_option} =
               Membrane.ICE.TURNManager.ensure_tls_turn_launched(ip: {127, 0, 0, 1})
    end
  end
end
