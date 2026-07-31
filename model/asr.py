import numpy as np
import torch, torchaudio
import re
import soxr


class ParaformerASR:
    def __init__(self):
        from modelscope.pipelines import pipeline
        from modelscope.utils.constant import Tasks

        try:
            self.asr_pipeline = pipeline(
                task=Tasks.auto_speech_recognition,
                model="iic/speech_paraformer-large-vad-punc_asr_nat-zh-cn-16k-common-vocab8404-pytorch",
                model_revision="v2.0.4",
                device=f"cuda",
                disable_pbar=True,
                disable_update=True,
            )
        except Exception as e:
            print("Failed to load Paraformer model:", e)
            self.asr_pipeline = pipeline(
                task=Tasks.auto_speech_recognition,
                model="iic/speech_paraformer-large-vad-punc_asr_nat-zh-cn-16k-common-vocab8404-pytorch",
                device=f"cuda",
                disable_pbar=True,
                disable_update=True,
            )

    def recognize(self, audio_chunk, sample_rate=16000):
        if audio_chunk.ndim > 1:
            audio_chunk = audio_chunk.mean(axis=1)
        if sample_rate != 16000:
            audio_chunk = soxr.resample(audio_chunk, sample_rate, 16000)

        try:
            result = self.asr_pipeline(audio_chunk)[0]["text"].strip()
        except Exception as e:
            print("ASR recognition failed:", e)
            result = ""
        return result


class SensevoiceASR:
    def __init__(self, language="auto"):
        from funasr import AutoModel
        from funasr.utils.postprocess_utils import rich_transcription_postprocess

        model_dir = "iic/SenseVoiceSmall"

        self.sensevoice_model = AutoModel(
            model=model_dir,
            trust_remote_code=False,
            device=f"cuda",
            disable_pbar=True,
            disable_update=True,
        )
        self.language = language

        remove_set = {
            "😊",
            "😔",
            "😡",
            "😰",
            "🤢",
            "😮",
            "🎼",
            "👏",
            "😀",
            "😭",
            "🤧",
            "😷",
        }

        self.pattern = "[" + "".join(remove_set) + "]"
        self.rich_transcription_postprocess = rich_transcription_postprocess

    def clean_sensevoice_text(self, s: str) -> str:
        if not re.search(r"[\u4e00-\u9fff]|[a-zA-Z]", s):
            return ""
        return re.sub(self.pattern, "", s)

    def recognize(self, audio_chunk, sample_rate=16000, language=None):
        if audio_chunk.ndim > 1:
            audio_chunk = audio_chunk.mean(axis=1)
        if sample_rate != 16000:
            audio_chunk = soxr.resample(audio_chunk, sample_rate, 16000)

        if language is None:
            language = self.language

        try:
            raw = self.sensevoice_model.generate(
                input=audio_chunk,
                cache={},
                language=language,  # "zh", "en", "yue", "ja", "ko", "nospeech"
                use_itn=True,
                batch_size=16,
            )[0]["text"]
            # Extract language tag (e.g. <|zh|>, <|en|>) from raw output
            lang_tag = ""
            m = re.match(r"<\|[a-z]+\|>", raw)
            if m:
                lang_tag = m.group()
            result = self.rich_transcription_postprocess(raw).strip()
            result = self.clean_sensevoice_text(result)
            print(f"[SensevoiceLang] {lang_tag} | Text: {result}")
        except Exception as e:
            print("ASR recognition failed:", e)
            result = ""
        return result


class WhisperASR:
    """Whisper-medium based ASR for cascade recognition."""

    def __init__(self, model_path="pretrained_models/whisper-medium"):
        from transformers import WhisperProcessor, WhisperForConditionalGeneration

        self.processor = WhisperProcessor.from_pretrained(model_path)
        self.model = WhisperForConditionalGeneration.from_pretrained(model_path)
        self.model.to("cuda")
        # Auto-detect language (no forced_decoder_ids)
        self.model.config.forced_decoder_ids = None

    def recognize(self, audio_chunk, sample_rate=16000):
        try:
            if audio_chunk.ndim > 1:
                audio_chunk = audio_chunk.mean(axis=1)
            if sample_rate != 16000:
                audio_chunk = soxr.resample(audio_chunk, sample_rate, 16000)

            input_features = self.processor(
                audio_chunk, sampling_rate=16000, return_tensors="pt"
            ).input_features.to("cuda")

            with torch.no_grad():
                predicted_ids = self.model.generate(input_features)
            # Print detected language tag + text
            lang_id = predicted_ids[0, 1].item()
            lang = self.processor.tokenizer.decode([lang_id]).strip()
            text = self.processor.batch_decode(
                predicted_ids, skip_special_tokens=True
            )[0].strip()
            print(f"[WhisperLang] {lang} | Text: {text}")
        except Exception as e:
            print("Whisper ASR recognition failed:", e)
            text = ""
        return text
